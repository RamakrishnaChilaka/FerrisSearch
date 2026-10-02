use std::time::Duration;
use tokio::time::Instant;
use tonic::{Request, Status};

pub(crate) const DEFAULT_FORWARD_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const COPY_REFRESH_TIMEOUT: Duration = Duration::from_secs(5);
const REPLY_MARGIN: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug)]
pub(crate) struct RefreshDeadline {
    expires: Instant,
}

impl RefreshDeadline {
    pub(crate) fn from_request<T>(request: &Request<T>) -> Result<Self, Status> {
        let timeout = match request.metadata().get("grpc-timeout") {
            Some(value) => {
                let value = value.to_str().map_err(|error| {
                    Status::invalid_argument(format!("invalid grpc-timeout: {error}"))
                })?;
                parse_timeout(value)?
            }
            None => DEFAULT_FORWARD_TIMEOUT,
        };
        let expires = Instant::now()
            .checked_add(timeout.min(DEFAULT_FORWARD_TIMEOUT))
            .ok_or_else(|| Status::invalid_argument("grpc-timeout deadline overflows"))?;
        Ok(Self { expires })
    }

    pub(crate) fn remaining(self) -> Duration {
        let remaining = self.expires.saturating_duration_since(Instant::now());
        remaining.saturating_sub(REPLY_MARGIN.min(remaining / 4))
    }

    pub(crate) fn copy_budget(self, limit: Duration) -> Duration {
        self.remaining().min(limit)
    }
}

fn parse_timeout(value: &str) -> Result<Duration, Status> {
    let invalid = || Status::invalid_argument(format!("invalid grpc-timeout [{value}]"));
    let (unit, digits) = value.as_bytes().split_last().ok_or_else(invalid)?;
    if digits.is_empty() || digits.len() > 8 || !digits.iter().all(u8::is_ascii_digit) {
        return Err(invalid());
    }
    let number = std::str::from_utf8(digits)
        .map_err(|_| invalid())?
        .parse::<u64>()
        .map_err(|_| invalid())?;
    if number == 0 {
        return Err(invalid());
    }
    match unit {
        b'H' => Ok(Duration::from_secs(number * 3600)),
        b'M' => Ok(Duration::from_secs(number * 60)),
        b'S' => Ok(Duration::from_secs(number)),
        b'm' => Ok(Duration::from_millis(number)),
        b'u' => Ok(Duration::from_micros(number)),
        b'n' => Ok(Duration::from_nanos(number)),
        _ => Err(invalid()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_deadline_accepts_grpc_units_and_rejects_malformed_values() {
        for (value, expected) in [
            ("1H", Duration::from_secs(3600)),
            ("2M", Duration::from_secs(120)),
            ("3S", Duration::from_secs(3)),
            ("4m", Duration::from_millis(4)),
            ("5u", Duration::from_micros(5)),
            ("6n", Duration::from_nanos(6)),
        ] {
            assert_eq!(parse_timeout(value).unwrap(), expected);
        }
        for value in [
            "",
            "S",
            "0S",
            "-1S",
            "+1S",
            "oneS",
            "123456789S",
            "1x",
            "éS",
        ] {
            assert!(parse_timeout(value).is_err(), "{value}");
        }
    }

    #[test]
    fn refresh_budget_is_capped_and_reserves_reply_time_after_elapsed_work() {
        let mut request = Request::new(());
        request.set_timeout(Duration::from_secs(2));
        let deadline = RefreshDeadline::from_request(&request).unwrap();
        assert!(deadline.copy_budget(COPY_REFRESH_TIMEOUT) < Duration::from_secs(2));
        assert!(deadline.copy_budget(Duration::from_millis(100)) <= Duration::from_millis(100));
        let elapsed = RefreshDeadline {
            expires: Instant::now() + Duration::from_millis(40),
        };
        assert!(elapsed.remaining() <= Duration::from_millis(30));
        let expired = RefreshDeadline {
            expires: Instant::now() - Duration::from_secs(1),
        };
        assert_eq!(expired.copy_budget(COPY_REFRESH_TIMEOUT), Duration::ZERO);
    }

    #[test]
    fn refresh_request_deadline_rejects_invalid_metadata_and_caps_large_timeouts() {
        let mut request = Request::new(());
        for value in ["0S", "1x", "123456789S", "not-a-timeout"] {
            request
                .metadata_mut()
                .insert("grpc-timeout", value.parse().unwrap());
            assert_eq!(
                RefreshDeadline::from_request(&request).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
        request
            .metadata_mut()
            .insert("grpc-timeout", "1H".parse().unwrap());
        let deadline = RefreshDeadline::from_request(&request).unwrap();
        assert!(deadline.remaining() <= DEFAULT_FORWARD_TIMEOUT - REPLY_MARGIN);
        assert!(deadline.remaining() > Duration::from_secs(29));
        assert_eq!(
            deadline.copy_budget(COPY_REFRESH_TIMEOUT),
            COPY_REFRESH_TIMEOUT
        );
    }
}
