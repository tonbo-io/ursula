//! The `*at` calls on the open host directory: every step after its open names a file relative to
//! the descriptor, never by path, so a path swapped after the open cannot redirect a step.
#![expect(unsafe_code, reason = "std has no stable API for the *at calls")]

use std::ffi::CString;
use std::ffi::c_int;
use std::ffi::c_uint;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;

/// `name` as a C string. Every name here is ASCII the extension made, so a NUL never occurs.
fn c_name(name: &str) -> io::Result<CString> {
    CString::new(name).map_err(|_nul| io::Error::from(io::ErrorKind::InvalidInput))
}

fn check(rc: c_int) -> io::Result<()> {
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Opens `name` in `dir` with `flags` (and `O_CLOEXEC`), creating it with `mode` when `flags`
/// say so.
pub(super) fn open(dir: &File, name: &str, flags: c_int, mode: c_uint) -> io::Result<File> {
    let name = c_name(name)?;
    let flags = flags | libc::O_CLOEXEC;
    // SAFETY: `dir` is an open descriptor for the length of the call and `name` a NUL-terminated
    // string that outlives it. The mode is passed as the `unsigned int` `openat` reads.
    let fd = unsafe { libc::openat(dir.as_raw_fd(), name.as_ptr(), flags, mode) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` is a descriptor `openat` just returned, owned by nothing else.
    let file = unsafe { File::from_raw_fd(fd) };
    Ok(file)
}

/// Links `from` in `dir` as `to` in `dir`, never following a link and never replacing a file.
pub(super) fn link(dir: &File, from: &str, to: &str) -> io::Result<()> {
    let (from, to) = (c_name(from)?, c_name(to)?);
    let fd = dir.as_raw_fd();
    // SAFETY: `fd` is an open descriptor for the length of the call and both names are
    // NUL-terminated strings that outlive it.
    let rc = unsafe { libc::linkat(fd, from.as_ptr(), fd, to.as_ptr(), 0) };
    check(rc)
}

/// Removes the file `name` from `dir`.
pub(super) fn unlink(dir: &File, name: &str) -> io::Result<()> {
    let name = c_name(name)?;
    // SAFETY: `dir` is an open descriptor for the length of the call and `name` a NUL-terminated
    // string that outlives it.
    let rc = unsafe { libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), 0) };
    check(rc)
}
