pub enum FlushOutcome {
    Committed,
    InsertFailed,
    Empty,
}

pub fn try_flush<T>(
    buf: &mut Vec<T>,
    insert_result: Result<(), String>,
    commit: impl FnOnce() -> Result<(), String>,
) -> FlushOutcome {
    if buf.is_empty() {
        return FlushOutcome::Empty;
    }
    match insert_result {
        Ok(()) => {
            buf.clear();
            if let Err(e) = commit() {
                tracing::warn!(error = %e, "commit failed after successful insert");
            }
            FlushOutcome::Committed
        }
        Err(e) => {
            tracing::warn!(error = %e, "insert failed, keeping batch");
            FlushOutcome::InsertFailed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;

    #[test]
    fn flush_success_clears_and_commits() {
        let mut buf = vec![1, 2, 3];
        let commit_called = Rc::new(Cell::new(false));
        let cc = commit_called.clone();

        let outcome = try_flush(&mut buf, Ok(()), || {
            cc.set(true);
            Ok(())
        });

        assert!(matches!(outcome, FlushOutcome::Committed));
        assert!(buf.is_empty());
        assert!(commit_called.get());
    }

    #[test]
    fn flush_failure_keeps_batch_and_no_commit() {
        let mut buf = vec![1, 2, 3];
        let commit_called = Rc::new(Cell::new(false));
        let cc = commit_called.clone();

        let outcome = try_flush(&mut buf, Err("insert failed".into()), || {
            cc.set(true);
            Ok(())
        });

        assert!(matches!(outcome, FlushOutcome::InsertFailed));
        assert_eq!(buf.len(), 3);
        assert!(!commit_called.get());
    }

    #[test]
    fn flush_empty_returns_empty() {
        let mut buf: Vec<i32> = vec![];
        let outcome = try_flush(&mut buf, Ok(()), || Ok(()));
        assert!(matches!(outcome, FlushOutcome::Empty));
    }

    #[test]
    fn bad_payload_deserialize_fails() {
        let bad = b"not json at all";
        let result = serde_json::from_slice::<serde_json::Value>(bad);
        assert!(result.is_err());
    }
}
