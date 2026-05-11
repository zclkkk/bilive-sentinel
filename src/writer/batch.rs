use bilive_sentinel::redpanda::TopicPartition;
use std::collections::HashMap;

pub enum FlushOutcome {
    Committed,
    InsertFailed,
    CommitFailed,
    Empty,
}

pub struct PendingBatch<T> {
    rows: Vec<T>,
    offsets: HashMap<TopicPartition, i64>,
    inserted: bool,
}

impl<T> PendingBatch<T> {
    pub fn new() -> Self {
        Self {
            rows: Vec::new(),
            offsets: HashMap::new(),
            inserted: false,
        }
    }

    pub fn push(&mut self, row: T, topic: &str, partition: i32, next_offset: i64) {
        debug_assert!(!self.inserted);
        self.rows.push(row);
        self.advance_offset(topic, partition, next_offset);
    }

    pub fn advance_offset(&mut self, topic: &str, partition: i32, next_offset: i64) {
        self.offsets
            .entry(TopicPartition::new(topic, partition))
            .and_modify(|offset| *offset = (*offset).max(next_offset))
            .or_insert(next_offset);
    }

    pub fn rows(&self) -> &[T] {
        &self.rows
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn has_pending_offsets(&self) -> bool {
        !self.offsets.is_empty()
    }

    pub fn inserted(&self) -> bool {
        self.inserted
    }

    pub fn offsets(&self) -> &HashMap<TopicPartition, i64> {
        &self.offsets
    }

    fn mark_inserted(&mut self) {
        self.inserted = true;
    }

    fn clear(&mut self) {
        self.rows.clear();
        self.offsets.clear();
        self.inserted = false;
    }
}

pub fn try_flush<T>(
    batch: &mut PendingBatch<T>,
    insert_result: Option<Result<(), String>>,
    commit: impl FnOnce(&HashMap<TopicPartition, i64>) -> Result<(), String>,
) -> FlushOutcome {
    if !batch.has_pending_offsets() {
        return FlushOutcome::Empty;
    }

    // Has rows but not yet inserted — must insert first
    if !batch.is_empty() && !batch.inserted() {
        match insert_result {
            Some(Ok(())) => batch.mark_inserted(),
            Some(Err(e)) => {
                tracing::warn!(error = %e, "insert failed, keeping batch");
                return FlushOutcome::InsertFailed;
            }
            None => {
                tracing::warn!("insert result missing for uninserted batch");
                return FlushOutcome::InsertFailed;
            }
        }
    }

    if let Err(e) = commit(batch.offsets()) {
        tracing::warn!(error = %e, "commit failed");
        return FlushOutcome::CommitFailed;
    }

    batch.clear();
    FlushOutcome::Committed
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;

    #[test]
    fn flush_success_clears_and_commits() {
        let mut batch = PendingBatch::new();
        batch.push(1, "topic", 0, 1);
        batch.push(2, "topic", 0, 2);
        batch.push(3, "topic", 1, 1);
        let commit_called = Rc::new(Cell::new(false));
        let cc = commit_called.clone();

        let outcome = try_flush(&mut batch, Some(Ok(())), |offsets| {
            cc.set(true);
            assert_eq!(offsets.len(), 2);
            Ok(())
        });

        assert!(matches!(outcome, FlushOutcome::Committed));
        assert!(batch.is_empty());
        assert!(!batch.inserted());
        assert!(commit_called.get());
    }

    #[test]
    fn flush_failure_keeps_batch_and_no_commit() {
        let mut batch = PendingBatch::new();
        batch.push(1, "topic", 0, 1);
        let commit_called = Rc::new(Cell::new(false));
        let cc = commit_called.clone();

        let outcome = try_flush(&mut batch, Some(Err("insert failed".into())), |_| {
            cc.set(true);
            Ok(())
        });

        assert!(matches!(outcome, FlushOutcome::InsertFailed));
        assert_eq!(batch.len(), 1);
        assert!(!batch.inserted());
        assert!(!commit_called.get());
    }

    #[test]
    fn commit_failure_keeps_batch_visible() {
        let mut batch = PendingBatch::new();
        batch.push(1, "topic", 0, 1);

        let outcome = try_flush(&mut batch, Some(Ok(())), |_| Err("commit failed".into()));

        assert!(matches!(outcome, FlushOutcome::CommitFailed));
        assert_eq!(batch.len(), 1);
        assert!(batch.inserted());
    }

    #[test]
    fn commit_retry_skips_insert_and_clears_batch() {
        let mut batch = PendingBatch::new();
        batch.push(1, "topic", 0, 1);

        let outcome = try_flush(&mut batch, Some(Ok(())), |_| Err("commit failed".into()));
        assert!(matches!(outcome, FlushOutcome::CommitFailed));

        let outcome = try_flush(&mut batch, None, |_| Ok(()));

        assert!(matches!(outcome, FlushOutcome::Committed));
        assert!(batch.is_empty());
        assert!(!batch.inserted());
    }

    #[test]
    fn tracks_highest_offset_per_partition() {
        let mut batch = PendingBatch::new();
        batch.push(1, "topic", 0, 2);
        batch.push(2, "topic", 0, 4);
        batch.push(3, "topic", 0, 3);
        batch.push(4, "topic", 1, 7);

        assert_eq!(
            batch.offsets().get(&TopicPartition::new("topic", 0)),
            Some(&4)
        );
        assert_eq!(
            batch.offsets().get(&TopicPartition::new("topic", 1)),
            Some(&7)
        );
    }

    #[test]
    fn flush_empty_returns_empty() {
        let mut batch: PendingBatch<i32> = PendingBatch::new();
        let outcome = try_flush(&mut batch, Some(Ok(())), |_| Ok(()));
        assert!(matches!(outcome, FlushOutcome::Empty));
    }

    #[test]
    fn advance_offset_without_row() {
        let mut batch: PendingBatch<i32> = PendingBatch::new();
        batch.advance_offset("topic", 0, 5);
        batch.advance_offset("topic", 0, 3); // lower, should not overwrite
        batch.advance_offset("topic", 1, 7);

        assert!(batch.is_empty()); // no rows
        assert!(batch.has_pending_offsets());
        assert_eq!(
            batch.offsets().get(&TopicPartition::new("topic", 0)),
            Some(&5)
        );
        assert_eq!(
            batch.offsets().get(&TopicPartition::new("topic", 1)),
            Some(&7)
        );
    }

    #[test]
    fn offset_only_flush_commits_and_clears() {
        let mut batch: PendingBatch<i32> = PendingBatch::new();
        batch.advance_offset("topic", 0, 5);
        batch.advance_offset("topic", 1, 7);

        let commit_called = Rc::new(Cell::new(false));
        let cc = commit_called.clone();

        let outcome = try_flush(&mut batch, None, |offsets| {
            cc.set(true);
            assert_eq!(offsets.len(), 2);
            Ok(())
        });

        assert!(matches!(outcome, FlushOutcome::Committed));
        assert!(batch.is_empty());
        assert!(!batch.has_pending_offsets());
        assert!(!batch.inserted());
        assert!(commit_called.get());
    }

    #[test]
    fn bad_payload_deserialize_fails() {
        let bad = b"not json at all";
        let result = serde_json::from_slice::<serde_json::Value>(bad);
        assert!(result.is_err());
    }
}
