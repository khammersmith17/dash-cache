use std::time::Duration;
use std::time::Instant;

pub(crate) fn expires_from_ttl(ttl: Option<Duration>) -> Option<Instant> {
    let Some(dur) = ttl else { return None };
    Some(Instant::now() + dur)
}

pub(crate) fn is_expired(t: &Instant) -> bool {
    Instant::now().ge(t)
}
