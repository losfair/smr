use std::{iter::FromIterator, time::Instant};

use anyhow::Result;
use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use futures::{stream::FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use rand::Rng;
use serde::{Deserialize, Serialize};
use smr::{
  pm::{pm_start, PmHandle, WorkerManager},
  scheduler::Scheduler,
  types::{BaseRequest, Request, Response},
};
use tokio::runtime::Runtime;

#[derive(Serialize, Deserialize)]
struct EchoReq {
  x: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct EchoRes {
  x: Vec<u8>,
}

impl BaseRequest for EchoReq {
  type Res = EchoRes;
  type InitData = ();
  type Context = ();
}

#[async_trait(?Send)]
impl Request for EchoReq {
  async fn handle(self, _: &'static ()) -> Result<Self::Res> {
    Ok(EchoRes { x: self.x })
  }

  async fn init(_: ()) -> &'static () {
    &()
  }
}

impl Response for EchoRes {}

#[ctor::ctor]
static PM: Mutex<PmHandle<EchoReq>> = Mutex::new(
  #[allow(unused_unsafe)]
  unsafe {
    pm_start()
  },
);

fn bench_roundtrip(c: &mut Criterion) {
  let rt = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .unwrap();
  let pm: PmHandle<EchoReq> = (*PM).lock().clone();
  let w = rt.block_on(pm.start_worker(())).unwrap().grab();
  c.bench_function("invoke roundtrip 16b", |b| {
    let payload = vec![0u8; 16];
    b.iter(|| {
      let res = rt
        .block_on(w.invoke(EchoReq { x: payload.clone() }))
        .unwrap();
      assert_eq!(res.x, payload);
    })
  });
  c.bench_function("invoke roundtrip 1024b", |b| {
    let payload = vec![0u8; 1024];
    b.iter(|| {
      let res = rt
        .block_on(w.invoke(EchoReq { x: payload.clone() }))
        .unwrap();
      assert_eq!(res.x, payload);
    })
  });
  c.bench_function("concurrent invoke 16b", |b| {
    run_concurrent_invoke_bench(&rt, &w, b, vec![0u8; 16])
  });
  c.bench_function("concurrent invoke 1024b", |b| {
    run_concurrent_invoke_bench(&rt, &w, b, vec![0u8; 1024])
  });
}

fn bench_sched(c: &mut Criterion) {
  let pm = PM.lock().clone();
  let rt = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .unwrap();
  let sched = rt.block_on(async { Scheduler::<u64, _>::new(pm) });
  c.bench_function("get worker (4 apps)", |b| {
    b.iter(|| {
      let appid: u64 = rand::thread_rng().gen_range(0..4);
      rt.block_on(Scheduler::get_worker(&sched, &appid, || ()))
        .unwrap();
    });
  });
  c.bench_function("get worker (256 apps)", |b| {
    b.iter(|| {
      let appid: u64 = rand::thread_rng().gen_range(0..256);
      rt.block_on(Scheduler::get_worker(&sched, &appid, || ()))
        .unwrap();
    });
  });
  c.bench_function("sched invoke (single thread)", |b| {
    b.iter(|| {
      let mut buf = vec![0u8; 16];
      rand::thread_rng().fill(&mut buf[..]);
      let worker = rt
        .block_on(Scheduler::get_worker(&sched, &0u64, || ()))
        .unwrap();
      let res = rt
        .block_on(worker.invoke(EchoReq { x: buf.clone() }))
        .unwrap();
      assert_eq!(res.x, buf);
    });
  });
  c.bench_function("sched invoke (concurrent)", |b| {
    b.iter_custom(|count| {
      let start = Instant::now();
      let fut = std::iter::repeat(vec![0u8; 16])
        .take(count as usize)
        .map(|x| {
          let sched = sched.clone();
          rt.spawn(async move {
            let w = Scheduler::get_worker(&sched, &0u64, || ()).await.unwrap();
            w.invoke(EchoReq { x }).await.unwrap();
            ()
          })
        })
        .collect::<Vec<_>>();
      let _: Vec<_> = rt.block_on(FuturesUnordered::from_iter(fut.into_iter()).collect());
      start.elapsed()
    });
  });
}

fn run_concurrent_invoke_bench(
  rt: &Runtime,
  w: &WorkerManager<EchoReq>,
  b: &mut Bencher,
  material: Vec<u8>,
) {
  b.iter_custom(|count| {
    let start = Instant::now();
    let fut = FuturesUnordered::from_iter(
      std::iter::repeat(material.clone())
        .take(count as usize)
        .map(|x| w.invoke(EchoReq { x })),
    )
    .collect();
    let res: Vec<_> = rt.block_on(fut);
    assert_eq!(res.len(), count as usize);
    start.elapsed()
  });
}

fn bench_worker_creation(c: &mut Criterion) {
  let rt = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .unwrap();
  let pm: PmHandle<EchoReq> = (*PM).lock().clone();
  c.bench_function("start worker", |b| {
    b.iter(|| {
      rt.block_on(pm.start_worker(())).unwrap().grab();
    })
  });
}

criterion_group!(benches, bench_sched, bench_roundtrip, bench_worker_creation);
criterion_main!(benches);
