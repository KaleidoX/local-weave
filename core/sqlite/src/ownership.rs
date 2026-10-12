use crate::{Error, Result};
use std::fs::File;

// The caller keeps the File alive until after the SQLite connection is closed.
// Dropping it also releases ownership when opening the store fails partway through.
pub(crate) fn acquire(file: &File) -> Result<()> {
    match try_lock(file) {
        Ok(()) => Ok(()),
        Err(std::fs::TryLockError::WouldBlock) => Err(Error::DatabaseInUse),
        Err(std::fs::TryLockError::Error(error)) => Err(Error::Io(error)),
    }
}

#[cfg(not(target_os = "android"))]
fn try_lock(file: &File) -> std::result::Result<(), std::fs::TryLockError> {
    file.try_lock()
}

#[cfg(target_os = "android")]
fn try_lock(file: &File) -> std::result::Result<(), std::fs::TryLockError> {
    use std::os::fd::AsRawFd;

    // SAFETY: file owns a live descriptor for this call; flock neither takes
    // ownership nor accesses Rust memory. Android's NDK provides this syscall.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::WouldBlock {
        Err(std::fs::TryLockError::WouldBlock)
    } else {
        Err(std::fs::TryLockError::Error(error))
    }
}
