use std::{ffi::CStr, ptr::NonNull, time::SystemTime};

use nix::{
  libc,
  unistd::{getppid, Pid},
};
use phf::phf_set;

static ALLOWED_ENV: phf::Set<&'static str> = phf_set! {
  "RUST_LOG",
  "RUST_BACKTRACE",
  "TERM",
  "SMR_STRACE",
};

static mut PROCESS_NAME_BUF: Option<NonNull<[u8]>> = None;
static mut ROOT_PID: Option<Pid> = None;
static mut PM_START_TIME: Option<SystemTime> = None;

pub unsafe fn pm_secure_entry_bootstrap(
  _argc: libc::c_int,
  argv: *mut *mut libc::c_char,
  envp: *mut *mut libc::c_char,
) {
  let argv_start = *argv;
  let argv_space = overwrite_argv_like(argv, |_| false);
  overwrite_argv_like(envp, |x| {
    x.starts_with("SMRAPP_") || ALLOWED_ENV.contains(x.split("=").next().unwrap())
  });
  PROCESS_NAME_BUF = Some(NonNull::from(std::slice::from_raw_parts_mut(
    argv_start as *mut u8,
    argv_space,
  )));

  let ppid = getppid();
  ROOT_PID = Some(ppid);
  PM_START_TIME = Some(SystemTime::now());

  set_worker_process_name("pm");
}

pub fn root_pid() -> i32 {
  unsafe { ROOT_PID.map(|x| x.as_raw()).unwrap_or(0) }
}

pub fn pm_start_time() -> SystemTime {
  unsafe { PM_START_TIME.unwrap_or_else(|| SystemTime::UNIX_EPOCH) }
}

pub fn set_worker_process_name(name: &str) {
  unsafe {
    if let Some(mut buf) = PROCESS_NAME_BUF {
      let name = format!(
        "smr[{}]: {}",
        ROOT_PID.map(|x| x.as_raw()).unwrap_or(0),
        name
      );
      let buf = buf.as_mut();
      let mut name = name.as_bytes();

      assert!(buf.len() > 0);
      if name.len() > buf.len() - 1 {
        name = &name[..buf.len() - 1];
      }

      buf[..name.len()].copy_from_slice(name);
      buf[name.len()..].fill(0);
    }
  }
}

unsafe fn overwrite_argv_like(
  arr: *mut *mut libc::c_char,
  mut allow: impl FnMut(&str) -> bool,
) -> usize {
  let mut sanitized: Vec<*mut libc::c_char> = vec![];
  let mut space = 0usize;

  let mut p = arr;
  while !(*p).is_null() {
    let cstr = CStr::from_ptr(*p);
    let byte_len = cstr.to_bytes_with_nul().len();
    let value = cstr.to_str().unwrap();
    if allow(value) {
      sanitized.push(*p);
    } else {
      // Overwrite with zeros
      std::ptr::write_bytes(*p, 0, byte_len);
    }
    space += byte_len;
    p = p.add(1);
  }
  sanitized.push(std::ptr::null_mut());
  std::ptr::copy_nonoverlapping(sanitized.as_ptr(), arr, sanitized.len());
  space
}
