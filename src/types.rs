use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

pub trait BaseRequest: Serialize + Send {
  type Res: Response;
  type InitData: InitData;
  type Context: 'static;
}

#[async_trait(?Send)]
pub trait Request: BaseRequest + for<'de> Deserialize<'de> + 'static {
  async fn init(_init_data: Self::InitData) -> &'static Self::Context;
  async fn handle(self, ctx: &'static Self::Context) -> Result<Self::Res>;
  async fn handle_with_cancellation(
    self,
    ctx: &'static Self::Context,
    _: watch::Receiver<()>,
  ) -> Result<Self::Res> {
    self.handle(ctx).await
  }
}

pub trait Response: Serialize + for<'de> Deserialize<'de> + Send + 'static {}

pub trait InitData: Serialize + for<'de> Deserialize<'de> + Send + 'static {
  fn process_name(&self) -> String {
    "worker".into()
  }
}

impl InitData for () {}
