use chrono::{Duration, NaiveDateTime};
use sqlx::PgPool;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomRunState {
    Runnable,
    Disabled,
    LeaseLost,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Room {
    pub room_id: i64,
    pub enabled: bool,
    pub last_connected_at: Option<NaiveDateTime>,
    pub last_error: Option<String>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct WorkerLease {
    pub room_id: i64,
    pub worker_id: String,
    pub leased_at: NaiveDateTime,
    pub expires_at: NaiveDateTime,
    pub last_heartbeat: NaiveDateTime,
}

pub async fn create_tables(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rooms (
            room_id BIGINT PRIMARY KEY,
            enabled BOOLEAN NOT NULL DEFAULT true,
            last_connected_at TIMESTAMP,
            last_error TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS worker_leases (
            room_id BIGINT PRIMARY KEY REFERENCES rooms(room_id),
            worker_id TEXT NOT NULL,
            leased_at TIMESTAMP NOT NULL DEFAULT NOW(),
            expires_at TIMESTAMP NOT NULL,
            last_heartbeat TIMESTAMP NOT NULL DEFAULT NOW()
        )",
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn add_room(pool: &PgPool, room_id: i64) -> Result<Room, sqlx::Error> {
    sqlx::query_as(
        "INSERT INTO rooms (room_id) VALUES ($1)
         ON CONFLICT (room_id) DO UPDATE SET updated_at = NOW()
         RETURNING *",
    )
    .bind(room_id)
    .fetch_one(pool)
    .await
}

pub async fn list_rooms(pool: &PgPool) -> Result<Vec<Room>, sqlx::Error> {
    sqlx::query_as("SELECT * FROM rooms ORDER BY room_id")
        .fetch_all(pool)
        .await
}

pub async fn set_room_enabled(
    pool: &PgPool,
    room_id: i64,
    enabled: bool,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let result =
        sqlx::query("UPDATE rooms SET enabled = $2, updated_at = NOW() WHERE room_id = $1")
            .bind(room_id)
            .bind(enabled)
            .execute(&mut *tx)
            .await?;
    if !enabled {
        sqlx::query("DELETE FROM worker_leases WHERE room_id = $1")
            .bind(room_id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(result.rows_affected() > 0)
}

pub async fn mark_room_connected(pool: &PgPool, room_id: i64) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE rooms
         SET last_connected_at = NOW(), last_error = NULL, updated_at = NOW()
         WHERE room_id = $1",
    )
    .bind(room_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_room_error(pool: &PgPool, room_id: i64, error: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE rooms
         SET last_error = $2, updated_at = NOW()
         WHERE room_id = $1",
    )
    .bind(room_id)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn claim_room(
    pool: &PgPool,
    room_id: i64,
    worker_id: &str,
    ttl: Duration,
) -> Result<Option<WorkerLease>, sqlx::Error> {
    let ttl_secs = ttl.num_seconds();
    sqlx::query_as(
        "INSERT INTO worker_leases (room_id, worker_id, leased_at, expires_at, last_heartbeat)
         SELECT $1, $2, NOW(), NOW() + make_interval(secs => $3), NOW()
         FROM rooms WHERE room_id = $1 AND enabled = true
         ON CONFLICT (room_id) DO UPDATE
         SET worker_id = EXCLUDED.worker_id,
             leased_at = EXCLUDED.leased_at,
             expires_at = EXCLUDED.expires_at,
             last_heartbeat = EXCLUDED.last_heartbeat
         WHERE worker_leases.expires_at < NOW()
         RETURNING *",
    )
    .bind(room_id)
    .bind(worker_id)
    .bind(ttl_secs as f64)
    .fetch_optional(pool)
    .await
}

pub async fn renew_lease(
    pool: &PgPool,
    room_id: i64,
    worker_id: &str,
    ttl: Duration,
) -> Result<bool, sqlx::Error> {
    let ttl_secs = ttl.num_seconds();
    let result = sqlx::query(
        "UPDATE worker_leases
         SET expires_at = NOW() + make_interval(secs => $3), last_heartbeat = NOW()
         WHERE room_id = $1
         AND worker_id = $2
         AND expires_at > NOW()
         AND EXISTS (
             SELECT 1 FROM rooms
             WHERE rooms.room_id = worker_leases.room_id
             AND rooms.enabled = true
         )",
    )
    .bind(room_id)
    .bind(worker_id)
    .bind(ttl_secs as f64)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn room_run_state(
    pool: &PgPool,
    room_id: i64,
    worker_id: &str,
) -> Result<RoomRunState, sqlx::Error> {
    let row: Option<(bool, Option<String>, Option<bool>)> = sqlx::query_as(
        "SELECT r.enabled,
                wl.worker_id,
                (wl.expires_at > NOW()) AS lease_current
         FROM rooms r
         LEFT JOIN worker_leases wl ON r.room_id = wl.room_id
         WHERE r.room_id = $1",
    )
    .bind(room_id)
    .fetch_optional(pool)
    .await?;

    let Some((enabled, lease_worker, lease_current)) = row else {
        return Ok(RoomRunState::LeaseLost);
    };
    if !enabled {
        return Ok(RoomRunState::Disabled);
    }
    if lease_worker.as_deref() == Some(worker_id) && lease_current.unwrap_or(false) {
        Ok(RoomRunState::Runnable)
    } else {
        Ok(RoomRunState::LeaseLost)
    }
}

pub async fn release_lease(
    pool: &PgPool,
    room_id: i64,
    worker_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM worker_leases WHERE room_id = $1 AND worker_id = $2")
        .bind(room_id)
        .bind(worker_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn release_all_leases(pool: &PgPool, worker_id: &str) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM worker_leases WHERE worker_id = $1")
        .bind(worker_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn claim_available_rooms(
    pool: &PgPool,
    worker_id: &str,
    capacity: usize,
    ttl: Duration,
) -> Result<Vec<WorkerLease>, sqlx::Error> {
    let room_ids: Vec<(i64,)> = sqlx::query_as(
        "SELECT r.room_id FROM rooms r
         LEFT JOIN worker_leases wl ON r.room_id = wl.room_id
         WHERE r.enabled = true
         AND (wl.room_id IS NULL OR wl.expires_at < NOW())
         ORDER BY r.room_id
         LIMIT $1",
    )
    .bind(capacity as i64)
    .fetch_all(pool)
    .await?;

    let mut leases = Vec::new();
    for (room_id,) in room_ids {
        if let Some(lease) = claim_room(pool, room_id, worker_id, ttl).await? {
            leases.push(lease);
        }
    }
    Ok(leases)
}

pub async fn list_leases(pool: &PgPool) -> Result<Vec<WorkerLease>, sqlx::Error> {
    sqlx::query_as("SELECT * FROM worker_leases ORDER BY room_id")
        .fetch_all(pool)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    async fn test_pool() -> PgPool {
        let pool = PgPool::connect("postgres://bilive:bilive@localhost:5432/bilive")
            .await
            .expect("connect postgres");
        create_tables(&pool).await.expect("create tables");
        // Clean up test data
        sqlx::query("DELETE FROM worker_leases")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM rooms")
            .execute(&pool)
            .await
            .unwrap();
        pool
    }

    #[tokio::test]
    #[serial]
    async fn add_and_list_rooms() {
        let pool = test_pool().await;
        let room = add_room(&pool, 12345).await.unwrap();
        assert_eq!(room.room_id, 12345);
        assert!(room.enabled);

        let rooms = list_rooms(&pool).await.unwrap();
        assert_eq!(rooms.len(), 1);
        assert_eq!(rooms[0].room_id, 12345);
    }

    #[tokio::test]
    #[serial]
    async fn test_set_room_enabled() {
        let pool = test_pool().await;
        add_room(&pool, 100).await.unwrap();
        claim_room(&pool, 100, "worker-1", Duration::seconds(60))
            .await
            .unwrap();
        super::set_room_enabled(&pool, 100, false).await.unwrap();

        let rooms = list_rooms(&pool).await.unwrap();
        assert!(!rooms[0].enabled);
        let leases = list_leases(&pool).await.unwrap();
        assert!(leases.is_empty());
    }

    #[tokio::test]
    #[serial]
    async fn claim_and_release() {
        let pool = test_pool().await;
        add_room(&pool, 200).await.unwrap();

        let lease = claim_room(&pool, 200, "worker-1", Duration::seconds(60))
            .await
            .unwrap();
        assert!(lease.is_some());
        let lease = lease.unwrap();
        assert_eq!(lease.worker_id, "worker-1");

        release_lease(&pool, 200, "worker-1").await.unwrap();
        let leases = list_leases(&pool).await.unwrap();
        assert!(leases.is_empty());
    }

    #[tokio::test]
    #[serial]
    async fn claim_disabled_room_fails() {
        let pool = test_pool().await;
        add_room(&pool, 300).await.unwrap();
        super::set_room_enabled(&pool, 300, false).await.unwrap();

        let lease = claim_room(&pool, 300, "worker-1", Duration::seconds(60))
            .await
            .unwrap();
        assert!(lease.is_none());
    }

    #[tokio::test]
    #[serial]
    async fn expired_lease_takeover() {
        let pool = test_pool().await;
        add_room(&pool, 400).await.unwrap();

        // Claim with very short TTL
        claim_room(&pool, 400, "worker-1", Duration::seconds(0))
            .await
            .unwrap();

        // Wait a moment for lease to expire
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Another worker claims expired lease
        let lease = claim_room(&pool, 400, "worker-2", Duration::seconds(60))
            .await
            .unwrap();
        assert!(lease.is_some());
        assert_eq!(lease.unwrap().worker_id, "worker-2");
    }

    #[tokio::test]
    #[serial]
    async fn renew_lease_success() {
        let pool = test_pool().await;
        add_room(&pool, 500).await.unwrap();
        claim_room(&pool, 500, "worker-1", Duration::seconds(60))
            .await
            .unwrap();

        let renewed = renew_lease(&pool, 500, "worker-1", Duration::seconds(120))
            .await
            .unwrap();
        assert!(renewed);
    }

    #[tokio::test]
    #[serial]
    async fn renew_disabled_room_fails() {
        let pool = test_pool().await;
        add_room(&pool, 550).await.unwrap();
        claim_room(&pool, 550, "worker-1", Duration::seconds(60))
            .await
            .unwrap();
        set_room_enabled(&pool, 550, false).await.unwrap();

        let renewed = renew_lease(&pool, 550, "worker-1", Duration::seconds(120))
            .await
            .unwrap();
        assert!(!renewed);
        assert_eq!(
            room_run_state(&pool, 550, "worker-1").await.unwrap(),
            RoomRunState::Disabled
        );
    }

    #[tokio::test]
    #[serial]
    async fn room_run_state_detects_lost_lease() {
        let pool = test_pool().await;
        add_room(&pool, 560).await.unwrap();
        claim_room(&pool, 560, "worker-1", Duration::seconds(60))
            .await
            .unwrap();

        assert_eq!(
            room_run_state(&pool, 560, "worker-1").await.unwrap(),
            RoomRunState::Runnable
        );
        assert_eq!(
            room_run_state(&pool, 560, "worker-2").await.unwrap(),
            RoomRunState::LeaseLost
        );
    }

    #[tokio::test]
    #[serial]
    async fn mark_room_status() {
        let pool = test_pool().await;
        add_room(&pool, 570).await.unwrap();

        mark_room_error(&pool, 570, "boom").await.unwrap();
        let room = list_rooms(&pool).await.unwrap().pop().unwrap();
        assert_eq!(room.last_error.as_deref(), Some("boom"));
        assert!(room.last_connected_at.is_none());

        mark_room_connected(&pool, 570).await.unwrap();
        let room = list_rooms(&pool).await.unwrap().pop().unwrap();
        assert!(room.last_error.is_none());
        assert!(room.last_connected_at.is_some());
    }

    #[tokio::test]
    #[serial]
    async fn release_all_leases_for_worker() {
        let pool = test_pool().await;
        add_room(&pool, 600).await.unwrap();
        add_room(&pool, 601).await.unwrap();
        claim_room(&pool, 600, "worker-1", Duration::seconds(60))
            .await
            .unwrap();
        claim_room(&pool, 601, "worker-1", Duration::seconds(60))
            .await
            .unwrap();

        release_all_leases(&pool, "worker-1").await.unwrap();
        let leases = list_leases(&pool).await.unwrap();
        assert!(leases.is_empty());
    }

    #[tokio::test]
    #[serial]
    async fn set_room_enabled_nonexistent_returns_false() {
        let pool = test_pool().await;
        assert!(!set_room_enabled(&pool, 999999, true).await.unwrap());
        assert!(!set_room_enabled(&pool, 999999, false).await.unwrap());
    }

    #[tokio::test]
    #[serial]
    async fn set_room_enabled_existing_returns_true() {
        let pool = test_pool().await;
        add_room(&pool, 700).await.unwrap();
        assert!(set_room_enabled(&pool, 700, false).await.unwrap());
        assert!(set_room_enabled(&pool, 700, true).await.unwrap());
    }
}
