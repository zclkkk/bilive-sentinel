use std::time::Duration;

pub fn calculate_backoff(base_ms: u64, max_ms: u64, attempt: u32, now_nanos: u64) -> Duration {
    let base = base_ms.min(max_ms);
    let exp = 2u64.saturating_pow(attempt.saturating_sub(1));
    let delay_ms = base.saturating_mul(exp).min(max_ms);
    let jitter_ms = now_nanos % (delay_ms / 4 + 1);
    Duration::from_millis(delay_ms + jitter_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_first_attempt() {
        let d = calculate_backoff(1000, 60000, 1, 0);
        assert!(d.as_millis() >= 1000 && d.as_millis() <= 1250);
    }

    #[test]
    fn backoff_doubles_each_attempt() {
        let d1 = calculate_backoff(1000, 60000, 1, 0);
        let d2 = calculate_backoff(1000, 60000, 2, 0);
        let d3 = calculate_backoff(1000, 60000, 3, 0);
        assert!(d2.as_millis() >= 2000 && d2.as_millis() <= 2500);
        assert!(d3.as_millis() >= 4000 && d3.as_millis() <= 5000);
        assert!(d1 < d2);
        assert!(d2 < d3);
    }

    #[test]
    fn backoff_caps_at_max() {
        let d = calculate_backoff(1000, 5000, 10, 0);
        assert!(d.as_millis() <= 5000 + 1250);
    }

    #[test]
    fn backoff_jitter_varies() {
        let d1 = calculate_backoff(1000, 60000, 1, 0);
        let d2 = calculate_backoff(1000, 60000, 1, 123_456_789);
        assert_ne!(d1, d2);
    }

    #[test]
    fn backoff_zero_base() {
        let d = calculate_backoff(0, 60000, 1, 0);
        assert_eq!(d, Duration::from_millis(0));
    }
}
