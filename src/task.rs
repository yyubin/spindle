use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// LocalTask는 특정 스레드에 고정되어 실행되는 Task입니다.
/// !Send Future를 받을 수 있어, 스레드 로컬 데이터 접근이 가능합니다.
pub struct LocalTask {
    future: Pin<Box<dyn Future<Output = ()>>>,
}

impl LocalTask {
    /// 새로운 LocalTask를 생성합니다.
    /// 고정된 스레드에서만 실행됩니다.
    pub fn local(future: impl Future<Output = ()> + 'static) -> Self {
        Self {
            future: Box::pin(future),
        }
    }

    /// Task를 폴링합니다.
    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.future.as_mut().poll(cx)
    }
}

/// SharedTask는 어떤 스레드에서든 실행될 수 있는 Task입니다.
/// Send Future만 받을 수 있어, 스레드 간 이동이 가능합니다.
pub struct SharedTask {
    future: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl SharedTask {
    /// 새로운 SharedTask를 생성합니다.
    /// 아무 스레드에서나 실행될 수 있습니다.
    pub fn shared(future: impl Future<Output = ()> + Send + 'static) -> Self {
        Self {
            future: Box::pin(future),
        }
    }

    /// Task를 폴링합니다.
    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.future.as_mut().poll(cx)
    }
}
