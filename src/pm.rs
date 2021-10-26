use std::{
  collections::HashMap,
  num::NonZeroU32,
  process::Command,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
  time::{Duration, SystemTime},
};

use anyhow::Result;
use ipc_channel::ipc::{self, IpcError};
use nix::{
  libc,
  sys::signal::{signal, SigHandler, Signal},
  unistd::{fork, getpid, ForkResult},
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
  sync::{
    mpsc::{unbounded_channel, UnboundedReceiver},
    oneshot, watch,
  },
  task::{spawn_blocking, spawn_local, LocalSet},
};

use crate::{
  config::PID_OVERRIDE,
  entry::{pm_secure_entry_bootstrap, pm_start_time, root_pid, set_worker_process_name},
  types::{BaseRequest, InitData, Request},
};

#[derive(Serialize, Deserialize)]
pub struct PmHandle<Req: BaseRequest> {
  wi: ipc::IpcSender<WorkerInit<Req>>,
}

impl<Req: BaseRequest> Clone for PmHandle<Req> {
  fn clone(&self) -> Self {
    Self {
      wi: self.wi.clone(),
    }
  }
}

pub struct WorkerHandle<Req: BaseRequest> {
  pid: i32,
  req_chan: ipc::IpcSender<TaskMessage<Req>>,
  res_chan: ipc::IpcReceiver<(u64, Result<Req::Res, String>)>,
}

pub struct WorkerManager<Req: Request> {
  pid: i32,
  req_chan: Mutex<ipc::IpcSender<TaskMessage<Req>>>,
  work_in_progress: WipType<Req::Res>,
  next_task_id: AtomicU64,
  termination: Arc<tokio::sync::RwLock<()>>,
}

type WipType<Res> = Arc<Mutex<Option<HashMap<u64, oneshot::Sender<Result<Res, String>>>>>>;

/// Start the process manager.
///
/// # Safety
///
/// This must be called before any threads are created.
pub unsafe fn pm_start<Req: Request>() -> PmHandle<Req> {
  do_pm_start(|| ())
}

/// Start the process manager, ensuring no environment variables or arguments are leaked.
///
/// # Safety
///
/// This must be called before any threads are created.
pub unsafe fn pm_secure_start<Req: Request>(
  argc: libc::c_int,
  argv: *mut *mut libc::c_char,
  envp: *mut *mut libc::c_char,
  process_init: impl FnOnce(),
) -> PmHandle<Req> {
  do_pm_start(move || {
    pm_secure_entry_bootstrap(argc, argv, envp);
    process_init();
  })
}

unsafe fn do_pm_start<Req: Request>(child_init: impl FnOnce()) -> PmHandle<Req> {
  // Inherited
  signal(Signal::SIGCHLD, SigHandler::SigIgn).unwrap();
  let (wi_tx, wi_rx) = ipc::channel().unwrap();
  if fork().unwrap().is_child() {
    prctl::set_death_signal(nix::libc::SIGTERM as isize).unwrap();
    drop(wi_tx);
    child_init();
    match pm_accept_entry(wi_rx) {}
  }
  PmHandle { wi: wi_tx }
}

impl<Req: Request> PmHandle<Req> {
  pub async fn start_worker(&self, init_data: Req::InitData) -> Result<WorkerHandle<Req>> {
    #[derive(Error, Debug)]
    #[error("worker initialization failed")]
    struct InitFailed;

    let (req_tx, req_rx) = ipc::channel()?;
    let (res_tx, res_rx) = ipc::channel()?;
    let (comp_tx, comp_rx) = ipc::channel()?;
    let wi = WorkerInit {
      init_completion: comp_tx,
      req_chan: req_rx,
      res_chan: res_tx,
      init_data,
    };
    self
      .wi
      .send(wi)
      .unwrap_or_else(|_| panic!("process manager crashed"));
    // The worker runs initialization routines before acking `init_completion`. So this may block indefinitely, and
    // we need to use `spawn_blocking` here to avoid blocking the event loop.
    let pid = spawn_blocking(move || comp_rx.recv().map_err(|_| InitFailed)).await??;
    Ok(WorkerHandle {
      pid,
      req_chan: req_tx,
      res_chan: res_rx,
    })
  }
}

impl<Req: Request> WorkerHandle<Req> {
  pub fn grab(self) -> WorkerManager<Req> {
    let term = Arc::new(tokio::sync::RwLock::new(()));
    let write_owner = term.clone().try_write_owned().unwrap();
    let wm = WorkerManager {
      pid: self.pid,
      req_chan: Mutex::new(self.req_chan),
      work_in_progress: Arc::new(Mutex::new(Some(HashMap::new()))),
      next_task_id: AtomicU64::new(1),
      termination: term,
    };
    let res_chan = self.res_chan;
    let work_in_progress = wm.work_in_progress.clone();
    std::thread::spawn(move || {
      // Guard to be dropped
      let _write_owner = write_owner;
      Self::relay_res(res_chan, work_in_progress);
    });
    wm
  }

  fn relay_res(
    res_chan: ipc::IpcReceiver<(u64, Result<Req::Res, String>)>,
    work_in_progress: WipType<Req::Res>,
  ) {
    loop {
      // `res_chan` must be drained as quickly as possible, since the sender uses a blocking call.
      let (task_id, res) = match res_chan.recv() {
        Ok(x) => x,
        Err(_) => {
          *work_in_progress.lock() = None;
          break;
        }
      };
      let completion = work_in_progress.lock().as_mut().unwrap().remove(&task_id);
      if let Some(x) = completion {
        let _ = x.send(res);
      }
    }
  }
}

impl<Req: Request> WorkerManager<Req> {
  pub fn pid(&self) -> i32 {
    self.pid
  }

  pub fn notify_on_termination<F: FnOnce() + Send + 'static>(&self, f: F) {
    let term = self.termination.clone();
    tokio::spawn(async move {
      term.read().await;
      f();
    });
  }

  pub async fn invoke(&self, req: Req) -> Result<Req::Res> {
    #[derive(Error, Debug)]
    #[error("invoke error: {0}")]
    struct InvokeError(String);

    #[derive(Error, Debug)]
    #[error("worker terminated")]
    struct WorkerTerminated;

    // Allocate task id.
    let task_id = self.next_task_id.fetch_add(1, Ordering::Relaxed);

    // Insert the response channel.
    let (tx, rx) = oneshot::channel();
    match &mut *self.work_in_progress.lock() {
      Some(x) => {
        x.insert(task_id, tx);
      }
      None => return Err(WorkerTerminated.into()),
    }

    // Prepare for completion.
    struct CompletionGuard<'a, Req: Request> {
      work_in_progress: &'a WipType<Req::Res>,
      task_id: u64,
      req_chan: &'a Mutex<ipc::IpcSender<TaskMessage<Req>>>,
      should_cancel: bool,
    }
    impl<'a, Req: Request> Drop for CompletionGuard<'a, Req> {
      fn drop(&mut self) {
        if let Some(x) = &mut *self.work_in_progress.lock() {
          x.remove(&self.task_id);
        }
        if self.should_cancel {
          let task_id = self.task_id;
          let _ = self
            .req_chan
            .lock()
            .send(TaskMessage::Interrupt { task_id });
        }
      }
    }
    let mut cg: CompletionGuard<Req> = CompletionGuard {
      work_in_progress: &self.work_in_progress,
      task_id,
      req_chan: &self.req_chan,
      should_cancel: true,
    };

    self.req_chan.lock().send(TaskMessage::Call {
      task_id,
      task_req: req,
    })?;
    let res = match rx.await {
      Ok(x) => x.map_err(|x| anyhow::Error::from(InvokeError(x))),
      Err(_) => Err(WorkerTerminated.into()),
    };
    cg.should_cancel = false;
    res
  }
}

#[derive(Serialize, Deserialize)]
struct WorkerInit<Req: BaseRequest> {
  init_completion: ipc::IpcSender<i32>,
  req_chan: ipc::IpcReceiver<TaskMessage<Req>>,
  res_chan: ipc::IpcSender<(u64, Result<Req::Res, String>)>,
  init_data: Req::InitData,
}

#[derive(Serialize, Deserialize)]
enum TaskMessage<Req> {
  Call { task_id: u64, task_req: Req },
  Interrupt { task_id: u64 },
}

fn pm_accept_entry<Req: Request>(ch: ipc::IpcReceiver<WorkerInit<Req>>) -> ! {
  // HACK: Logger of the parent process is initialized after PM in tests, but we want to print logs.
  #[cfg(test)]
  {
    let _ = pretty_env_logger::try_init_timed();
  }
  loop {
    let wi = ch.recv().map(Some).unwrap_or_else(|e| match e {
      IpcError::Bincode(e) => {
        log::error!(
          "pm_accept [{}]: deserialize error: {:?}",
          std::process::id(),
          e
        );
        None
      }
      IpcError::Io(e) => {
        log::error!(
          "pm_accept [{}]: io error, exiting: {:?}",
          std::process::id(),
          e
        );
        std::process::exit(1);
      }
      IpcError::Disconnected => {
        std::process::exit(0);
      }
    });
    let wi = match wi {
      Some(x) => x,
      None => continue,
    };
    let (tx, rx) = ipc::channel::<u32>().unwrap();
    match unsafe { fork() }.unwrap() {
      ForkResult::Child => {
        prctl::set_death_signal(nix::libc::SIGTERM as isize).unwrap();

        // Set PID override
        drop(tx);
        let global_pid = rx.recv().unwrap();
        PID_OVERRIDE.store(NonZeroU32::new(global_pid));

        set_worker_process_name(&wi.init_data.process_name());
        if std::env::var("SMR_STRACE").is_ok() {
          init_strace();
        }
        tokio::runtime::Builder::new_current_thread()
          .enable_all()
          .build()
          .unwrap()
          .block_on(async move {
            let local_set = LocalSet::new();
            local_set.run_until(pm_worker_entry(wi)).await
          });
        unreachable!()
      }
      ForkResult::Parent { child } => {
        let child = child.as_raw();
        assert!(child > 1);
        drop(rx);
        let _ = tx.send(child as u32);
      }
    }
  }
}

fn init_strace() {
  let pid = getpid().as_raw();
  let out_path = format!(
    "/tmp/smr_strace_{}_{}_{}_{}.log",
    pm_start_time()
      .duration_since(SystemTime::UNIX_EPOCH)
      .unwrap()
      .as_secs(),
    root_pid(),
    SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH)
      .unwrap()
      .as_secs(),
    pid
  );
  log::info!(
    "Invoking strace on pid {} with output path {}.",
    pid,
    out_path
  );
  Command::new("strace")
    .arg("-f")
    .arg("-o")
    .arg(&out_path)
    .arg("-p")
    .arg(&format!("{}", pid))
    .spawn()
    .unwrap();
  loop {
    if std::fs::File::open(&out_path).is_ok() {
      break;
    }
    std::thread::sleep(Duration::from_millis(10));
  }
  log::info!("strace is ready");
}

type TaskMap = Arc<Mutex<HashMap<u64, watch::Sender<()>>>>;
type FwdEntry<Req> = (u64, Req, watch::Receiver<()>);

async fn pm_worker_entry<Req: Request>(wi: WorkerInit<Req>) -> ! {
  let req_chan = wi.req_chan;
  let res_chan = wi.res_chan;
  let (req_fwd_tx, req_fwd_rx) = unbounded_channel::<FwdEntry<Req>>();
  let task_map: TaskMap = Arc::new(Mutex::new(HashMap::new()));
  let task_map_2 = task_map.clone();

  // Forward IPC requests to our process-local channel.
  // `req_chan` must be drained as quickly as possible, since the sender uses a blocking call.
  std::thread::spawn(move || loop {
    let msg = req_chan.recv().unwrap_or_else(|_| {
      log::debug!("pm_worker [{}]: exiting", std::process::id());
      std::process::exit(0)
    });

    match msg {
      TaskMessage::Call { task_id, task_req } => {
        log::debug!("pm_worker [{}]: call {}", getpid_with_override(), task_id);
        let (tx, rx) = watch::channel::<()>(());
        task_map.lock().insert(task_id, tx);
        let _ = req_fwd_tx.send((task_id, task_req, rx));
      }
      TaskMessage::Interrupt { task_id } => {
        log::debug!(
          "pm_worker [{}]: interrupt {}",
          getpid_with_override(),
          task_id
        );
        task_map.lock().remove(&task_id);
      }
    }
  });

  // Initialize worker environment.
  let ctx: &'static Req::Context = Req::init(wi.init_data).await;

  // If we failed to ack, it means that the sender side is dropped and we should exit now
  if wi.init_completion.send(getpid_with_override()).is_err() {
    std::process::exit(0);
  }
  drop(wi.init_completion);

  pm_async_entry(req_fwd_rx, res_chan, ctx, task_map_2).await
}

async fn pm_async_entry<Req: Request>(
  mut req_fwd_rx: UnboundedReceiver<FwdEntry<Req>>,
  res_chan: ipc::IpcSender<(u64, Result<Req::Res, String>)>,
  ctx: &'static Req::Context,
  task_map: TaskMap,
) -> ! {
  loop {
    let (task_id, req, cancellation) = req_fwd_rx.recv().await.expect("req_fwd_rx.recv() failed");
    let tasks = task_map.clone();
    let res_chan = res_chan.clone();
    spawn_local(async move {
      let mut cancellation_2 = cancellation.clone();
      tokio::select! {
        res = pm_async_handle(req, ctx, cancellation) => {
          tasks.lock().remove(&task_id);
          let res = res.map_err(|x| format!("{:?}", x));
          let _ = res_chan.send((task_id, res));
        }
        _ = cancellation_2.changed() => {
          // Interrupted
        }
      }
    });
  }
}

async fn pm_async_handle<Req: Request>(
  req: Req,
  ctx: &'static Req::Context,
  cancellation: watch::Receiver<()>,
) -> Result<Req::Res> {
  req.handle_with_cancellation(ctx, cancellation).await
}

fn getpid_with_override() -> i32 {
  if let Some(x) = PID_OVERRIDE.load() {
    x.get() as i32
  } else {
    nix::unistd::getpid().as_raw()
  }
}
