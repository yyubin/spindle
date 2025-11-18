use crate::task::LocalTask;
use crossbeam_deque::{Stealer, Worker as Deque};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

/// 워커 스레드의 상태
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    /// 유휴 상태 - 실행할 작업이 없음
    Idle,
    /// 실행 중 - 작업을 처리하고 있음
    Running,
    /// 중지됨 - 워커가 종료됨
    Stopped,
}

/// 워커 스레드
/// 내부 deque와 최우선 실행 슬롯(next)을 가집니다.
pub struct Worker {
    /// 워커 ID
    id: usize,
    /// 현재 상태
    state: WorkerState,
    /// 로컬 작업 큐 (work-stealing deque)
    local_queue: Deque<LocalTask>,
    /// 최우선 실행 작업 - Go의 runnext와 유사
    /// 이 슬롯에 있는 작업은 큐보다 먼저 실행됩니다.
    next: Option<LocalTask>,
}

impl Worker {
    /// 새로운 워커를 생성합니다.
    pub fn new(id: usize) -> Self {
        Self {
            id,
            state: WorkerState::Idle,
            local_queue: Deque::new_fifo(),
            next: None,
        }
    }

    /// 워커 ID를 반환합니다.
    pub fn id(&self) -> usize {
        self.id
    }

    /// 현재 상태를 반환합니다.
    pub fn state(&self) -> WorkerState {
        self.state
    }

    /// 상태를 변경합니다.
    pub fn set_state(&mut self, state: WorkerState) {
        self.state = state;
    }

    /// 작업을 로컬 큐에 푸시합니다.
    pub fn push(&mut self, task: LocalTask) {
        self.local_queue.push(task);
    }

    /// 최우선 실행 슬롯에 작업을 설정합니다.
    /// 기존 next 작업이 있으면 로컬 큐로 이동시킵니다.
    pub fn set_next(&mut self, task: LocalTask) {
        if let Some(old_next) = self.next.take() {
            // 기존 next 작업은 로컬 큐로 이동
            self.local_queue.push(old_next);
        }
        self.next = Some(task);
    }

    /// 다음 실행할 작업을 가져옵니다.
    /// 1. next 슬롯을 먼저 확인
    /// 2. 없으면 로컬 큐에서 pop
    fn get_next_task(&mut self) -> Option<LocalTask> {
        // 최우선 작업이 있으면 먼저 반환
        if let Some(task) = self.next.take() {
            return Some(task);
        }

        // 로컬 큐에서 가져오기
        self.local_queue.pop()
    }

    /// 워커를 한 번 실행합니다 (한 번의 작업 사이클).
    /// 작업이 있으면 true, 없으면 false를 반환합니다.
    pub fn run_once(&mut self) -> bool {
        if let Some(mut task) = self.get_next_task() {
            self.state = WorkerState::Running;

            // Waker 생성
            let waker = dummy_waker();
            let mut cx = Context::from_waker(&waker);

            // Task 실행
            match task.poll(&mut cx) {
                Poll::Ready(()) => {
                    // 작업 완료
                    println!("Worker {}: Task completed", self.id);
                }
                Poll::Pending => {
                    // 아직 완료되지 않음 - 다시 큐에 넣기
                    println!("Worker {}: Task pending, re-queuing", self.id);
                    self.local_queue.push(task);
                }
            }

            true
        } else {
            self.state = WorkerState::Idle;
            false
        }
    }

    /// 워커를 계속 실행합니다.
    /// 큐가 비어있으면 종료합니다.
    pub fn run(&mut self) {
        println!("Worker {} starting...", self.id);

        loop {
            if !self.run_once() {
                // 더 이상 작업이 없음
                println!("Worker {}: No more tasks, stopping", self.id);
                self.state = WorkerState::Stopped;
                break;
            }
        }
    }

    /// Stealer를 반환합니다 (work-stealing을 위해).
    pub fn stealer(&self) -> Stealer<LocalTask> {
        self.local_queue.stealer()
    }
}

// 테스트용 더미 waker
fn dummy_waker() -> Waker {
    fn no_op(_: *const ()) {}
    fn clone(_: *const ()) -> RawWaker {
        dummy_raw_waker()
    }

    fn dummy_raw_waker() -> RawWaker {
        RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(clone, no_op, no_op, no_op),
        )
    }

    unsafe { Waker::from_raw(dummy_raw_waker()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_creation() {
        let worker = Worker::new(0);
        assert_eq!(worker.id(), 0);
        assert_eq!(worker.state(), WorkerState::Idle);
    }

    #[test]
    fn test_worker_next_priority() {
        let mut worker = Worker::new(0);

        // 큐에 작업 추가
        worker.push(LocalTask::local(async {
            println!("Task from queue");
        }));

        // next 슬롯에 작업 추가
        worker.set_next(LocalTask::local(async {
            println!("Priority task");
        }));

        // next 작업이 먼저 실행되어야 함
        assert!(worker.run_once());
        assert_eq!(worker.state(), WorkerState::Running);
    }
}
