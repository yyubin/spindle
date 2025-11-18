use mio::{Events, Poll};
use std::time::Duration;

/// I/O Driver는 Reactor 패턴을 구현합니다.
/// mio Poll을 사용해 readiness events를 수집합니다.
pub struct IoDriver {
    poll: Poll,
    events: Events,
}

impl IoDriver {
    /// 새로운 IoDriver를 생성합니다.
    pub fn new() -> std::io::Result<Self> {
        let poll = Poll::new()?;
        let events = Events::with_capacity(1024);

        Ok(Self { poll, events })
    }

    /// Reactor loop를 실행합니다.
    /// 무한 루프 안에서 readiness events를 수집합니다.
    pub fn run(&mut self) -> std::io::Result<()> {
        println!("IoDriver started, entering reactor loop...");

        loop {
            // I/O readiness events를 대기합니다
            // timeout을 설정하여 주기적으로 다른 작업도 처리할 수 있게 합니다
            self.poll.poll(&mut self.events, Some(Duration::from_millis(100)))?;

            // 수집된 events를 처리합니다
            for event in self.events.iter() {
                self.handle_event(event);
            }
        }
    }

    /// 개별 event를 처리합니다.
    fn handle_event(&self, event: &mio::event::Event) {
        let token = event.token();

        println!("Event received - Token: {:?}", token);

        if event.is_readable() {
            println!("  -> Readable");
        }

        if event.is_writable() {
            println!("  -> Writable");
        }

        if event.is_error() {
            println!("  -> Error");
        }

        if event.is_read_closed() {
            println!("  -> Read closed");
        }

        if event.is_write_closed() {
            println!("  -> Write closed");
        }
    }

    /// Poll에 대한 참조를 반환합니다.
    /// 외부에서 event source를 등록할 때 사용합니다.
    pub fn poll(&self) -> &Poll {
        &self.poll
    }
}

impl Default for IoDriver {
    fn default() -> Self {
        Self::new().expect("Failed to create IoDriver")
    }
}
