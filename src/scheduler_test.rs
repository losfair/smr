use std::time::Duration;

use crate::{
  error::WorkerTerminated,
  pm::{pm_start, PmHandle},
  scheduler::Scheduler,
  types::{BaseRequest, InitData, Request, Response},
};
use anyhow::Result;
use async_trait::async_trait;
use ipc_channel::ipc::TryRecvError;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
enum GenReq {
  Fib(i32),
  Crash,
}

#[derive(Serialize, Deserialize, Debug)]
struct GenRes {
  value: u64,
}

#[derive(Serialize, Deserialize, Default)]
struct GenInitData {
  ch: Option<ipc_channel::ipc::IpcSender<()>>,
}

struct GenContext {
  _ch: Option<ipc_channel::ipc::IpcSender<()>>,
}

impl InitData for GenInitData {
  fn process_name(&self) -> String {
    "worker".into()
  }
}

impl BaseRequest for GenReq {
  type Res = GenRes;
  type InitData = GenInitData;
  type Context = GenContext;
}

fn fib(n: i32) -> i32 {
  if n == 1 || n == 2 {
    1
  } else {
    fib(n - 1) + fib(n - 2)
  }
}

#[async_trait(?Send)]
impl Request for GenReq {
  async fn handle(self, _: &'static GenContext) -> Result<Self::Res> {
    match self {
      Self::Fib(x) => Ok(GenRes {
        value: fib(x) as u64,
      }),
      Self::Crash => std::process::exit(1),
    }
  }

  async fn init(d: GenInitData) -> &'static GenContext {
    Box::leak(Box::new(GenContext { _ch: d.ch }))
  }
}

impl Response for GenRes {}

#[ctor::ctor]
static PM: Mutex<PmHandle<GenReq>> = Mutex::new(
  #[allow(unused_unsafe)]
  unsafe {
    pm_start()
  },
);

#[tokio::test]
async fn test_sched_scale_out_in() {
  let _ = pretty_env_logger::try_init_timed();
  let pm = PM.lock().clone();
  let sched = Scheduler::<u64, _>::new(pm);
  let mut handles = vec![];
  for _ in 0..8 {
    let sched = sched.clone();
    let h = tokio::spawn(async move {
      for _ in 0..30 {
        let w = Scheduler::get_worker(&sched, &0u64, || Default::default())
          .await
          .unwrap();
        let ret = w.invoke(GenReq::Fib(35)).await.unwrap();
        assert_eq!(ret.value, 9227465);
        tokio::time::sleep(Duration::from_millis(2)).await;
      }
    });
    handles.push(h);
  }
  for h in handles {
    h.await.unwrap();
  }
  assert!(Scheduler::get_num_workers_for_app(&sched, &0u64).await > 1);
  log::info!("Waiting for scaling in.");
  tokio::time::sleep(Duration::from_millis(5000)).await;
  assert_eq!(Scheduler::get_num_workers_for_app(&sched, &0u64).await, 1);
}

#[tokio::test]
async fn test_terminate_worker() {
  let _ = pretty_env_logger::try_init_timed();
  let pm = PM.lock().clone();
  let sched = Scheduler::<u64, _>::new(pm);
  let (tx, rx) = ipc_channel::ipc::channel::<()>().unwrap();
  let tx = Mutex::new(tx);
  let worker_handle = Scheduler::get_worker(&sched, &0u64, move || GenInitData {
    ch: Some(tx.lock().clone()),
  })
  .await
  .unwrap();
  assert_eq!(Scheduler::get_num_workers_for_app(&sched, &0u64).await, 1);
  let ret = Scheduler::terminate_worker(&sched, &0u64).await;
  assert_eq!(ret, true);
  assert_eq!(Scheduler::get_num_workers_for_app(&sched, &0u64).await, 0);
  tokio::time::sleep(Duration::from_millis(500)).await;

  // A worker should not be terminated as long as there are valid `WorkerManager` handles to it.
  assert!(worker_handle.invoke(GenReq::Fib(1)).await.is_ok());
  assert!(matches!(rx.try_recv().unwrap_err(), TryRecvError::Empty));
  drop(worker_handle);

  tokio::time::sleep(Duration::from_millis(500)).await;
  assert!(matches!(
    rx.try_recv().unwrap_err(),
    TryRecvError::IpcError(_)
  ));
}

#[tokio::test]
async fn test_sched_crash() {
  let _ = pretty_env_logger::try_init_timed();
  let pm = PM.lock().clone();
  let sched = Scheduler::<u64, _>::new(pm);
  let mut handles = vec![];
  for _ in 0..8 {
    let sched = sched.clone();
    let h = tokio::spawn(async move {
      for _ in 0..20 {
        let w = Scheduler::get_worker(&sched, &0u64, || Default::default()).await;
        if let Ok(w) = w {
          let ret = w.invoke(GenReq::Crash).await;
          assert!(ret
            .unwrap_err()
            .downcast_ref::<WorkerTerminated>()
            .is_some());
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
      }
    });
    handles.push(h);
  }
  for h in handles {
    h.await.unwrap();
  }
  tokio::time::sleep(Duration::from_millis(500)).await;
  log::info!("Testing recovery.");
  let mut handles = vec![];
  for _ in 0..8 {
    let sched = sched.clone();
    let h = tokio::spawn(async move {
      for _ in 0..30 {
        let w = Scheduler::get_worker(&sched, &0u64, || Default::default())
          .await
          .unwrap();
        let ret = w.invoke(GenReq::Fib(35)).await.unwrap();
        assert_eq!(ret.value, 9227465);
        tokio::time::sleep(Duration::from_millis(2)).await;
      }
    });
    handles.push(h);
  }
  for h in handles {
    h.await.unwrap();
  }
  assert!(Scheduler::get_num_workers_for_app(&sched, &0u64).await > 1);
}
