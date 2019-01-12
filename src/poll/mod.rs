use std::io;
use std::time::Duration;
use std::cmp;
use std::i32;

mod ready;
mod event;

pub use self::ready::Ready;
pub use self::event::{Event, Events};

pub fn poll(evts: &mut Events, timeout: Option<Duration>) -> io::Result<i32> {
    let timeout_ms = timeout
            .map(|to| cmp::min(millis(to), i32::MAX as u64) as i32)
            .unwrap_or(-1);

    let ret = unsafe { libc::poll(evts.events.as_mut_ptr(), evts.len() as u64, timeout_ms) };
    if ret < 0 {
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }

    Ok(ret)
}

const NANOS_PER_MILLI: u32 = 1_000_000;
const MILLIS_PER_SEC: u64 = 1_000;

pub fn millis(duration: Duration) -> u64 {
    let millis = (duration.subsec_nanos() + NANOS_PER_MILLI - 1) / NANOS_PER_MILLI;
    duration.as_secs().saturating_mul(MILLIS_PER_SEC).saturating_add(u64::from(millis))
}

