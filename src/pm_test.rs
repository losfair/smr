use std::time::Duration;

use crate::{
  pm::{pm_start, PmHandle},
  types::{BaseRequest, Request, Response},
};
use anyhow::Result;
use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct AddReq {
  a: u64,
  b: u64,
}

#[derive(Serialize, Deserialize)]
struct AddRes {
  value: u64,
}

impl BaseRequest for AddReq {
  type Res = AddRes;
  type InitData = ();
  type Context = ();
}

#[async_trait(?Send)]
impl Request for AddReq {
  async fn handle(self, _: &'static ()) -> Result<Self::Res> {
    Ok(AddRes {
      value: self.a + self.b,
    })
  }

  async fn init(_: ()) -> &'static () {
    &()
  }
}

impl Response for AddRes {}

#[derive(Serialize, Deserialize)]
struct SlowEchoReq {
  x: u64,
  delay: Duration,
}

#[derive(Serialize, Deserialize)]
struct SlowEchoRes {
  x: u64,
}

impl BaseRequest for SlowEchoReq {
  type Res = SlowEchoRes;
  type InitData = ();
  type Context = ();
}

#[async_trait(?Send)]
impl Request for SlowEchoReq {
  async fn handle(self, _: &'static ()) -> Result<Self::Res> {
    tokio::time::sleep(self.delay).await;
    Ok(SlowEchoRes { x: self.x })
  }

  async fn init(_: ()) -> &'static () {
    &()
  }
}

impl Response for SlowEchoRes {}

#[ctor::ctor]
static PM_ADD_REQ: Mutex<PmHandle<AddReq>> = Mutex::new(
  #[allow(unused_unsafe)]
  unsafe {
    pm_start()
  },
);

#[ctor::ctor]
static PM_SLOW_ECHO_REQ: Mutex<PmHandle<SlowEchoReq>> = Mutex::new(
  #[allow(unused_unsafe)]
  unsafe {
    pm_start()
  },
);

#[tokio::test]
async fn test_pm() {
  let _ = pretty_env_logger::try_init_timed();
  let pm: PmHandle<AddReq> = (*PM_ADD_REQ.lock()).clone();
  for _ in 0..500 {
    let w = pm.start_worker(()).await.unwrap().grab();
    let res = w.invoke(AddReq { a: 1, b: 2 }).await.unwrap();
    assert_eq!(res.value, 3);
  }
}

#[tokio::test]
async fn test_pm_slow_one_worker() {
  let _ = pretty_env_logger::try_init_timed();
  let pm: PmHandle<SlowEchoReq> = (*PM_SLOW_ECHO_REQ.lock()).clone();
  let w = pm.start_worker(()).await.unwrap().grab();
  for _ in 0..30 {
    tokio::select! {
        _ = w.invoke(SlowEchoReq {
          x: 42,
          delay: Duration::from_millis(15),
        }) => panic!("invoke should not return"),
      _ = tokio::time::sleep(Duration::from_millis(5)) => {
      }
    };
  }
}

#[tokio::test]
async fn test_pm_slow_many_workers() {
  let _ = pretty_env_logger::try_init_timed();
  let pm: PmHandle<SlowEchoReq> = (*PM_SLOW_ECHO_REQ.lock()).clone();
  for _ in 0..30 {
    let w = pm.start_worker(()).await.unwrap().grab();
    tokio::select! {
        _ = w.invoke(SlowEchoReq {
          x: 42,
          delay: Duration::from_millis(15),
        }) => panic!("invoke should not return"),
      _ = tokio::time::sleep(Duration::from_millis(5)) => {
      }
    };
  }
}
