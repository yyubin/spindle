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

    /// 다음 실행할 작업을 가져옴 (Go의 netpoller 참고)
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
    /// 작업이 있으면 true, 없으면 false를 반환
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

    /// 워커를 계속 실행
    /// 큐가 비어있으면 종료
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

    /// Stealer를 반환 (work-stealing)
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

/// 워커 스레드 풀 핸들
/// 워커 스레드를 관리하고 작업을 분배
use crate::task::{GameResponse, GameTask};
use crossbeam_channel::{Receiver, Sender};
use std::thread::{self, JoinHandle};

/// 워커 스레드 풀
/// 여러 워커 스레드를 관리하고 작업을 분배
pub struct WorkerPool {
    /// 워커 스레드 수
    worker_count: usize,
    /// 워커 스레드 핸들들
    worker_threads: Vec<JoinHandle<()>>,
    /// I/O -> Worker 채널 (작업 전달)
    task_sender: Sender<GameTask>,
    /// Worker -> I/O 채널 (응답 전달)
    response_receiver: Receiver<GameResponse>,
}

impl WorkerPool {
    /// 새로운 워커 풀을 생성
    ///
    /// # Arguments
    /// * `worker_count` - 생성할 워커 스레드 수
    ///
    /// Returns: (WorkerPool, task_receiver, response_sender)
    /// - task_receiver: I/O 스레드가 작업을 받는 수신 채널
    /// - response_sender: 워커가 응답을 보내는 송신 채널
    pub fn new(
        worker_count: usize,
    ) -> (
        Self,
        Receiver<GameTask>,
        Sender<GameResponse>,
    ) {
        // I/O -> Worker 채널 (unbounded)
        let (task_tx, task_rx) = crossbeam_channel::unbounded::<GameTask>();

        // Worker -> I/O 채널 (unbounded)
        let (response_tx, response_rx) = crossbeam_channel::unbounded::<GameResponse>();

        let mut worker_threads = Vec::with_capacity(worker_count);

        // 워커 스레드 생성
        for worker_id in 0..worker_count {
            let task_rx_clone = task_rx.clone();
            let response_tx_clone = response_tx.clone();

            let handle = thread::Builder::new()
                .name(format!("worker-{}", worker_id))
                .spawn(move || {
                    worker_thread_main(worker_id, task_rx_clone, response_tx_clone);
                })
                .expect("Failed to spawn worker thread");

            worker_threads.push(handle);
        }

        let pool = Self {
            worker_count,
            worker_threads,
            task_sender: task_tx,
            response_receiver: response_rx,
        };

        // I/O 스레드가 사용할 수신 채널과 송신 채널 반환
        (pool, task_rx, response_tx)
    }

    /// 작업을 워커에게 전달
    pub fn send_task(&self, task: GameTask) -> Result<(), crossbeam_channel::SendError<GameTask>> {
        self.task_sender.send(task)
    }

    /// 워커로부터 응답을 받기 (non-blocking)
    pub fn try_recv_response(&self) -> Result<GameResponse, crossbeam_channel::TryRecvError> {
        self.response_receiver.try_recv()
    }

    /// 워커 스레드 수를 반환
    pub fn worker_count(&self) -> usize {
        self.worker_count
    }

    /// 워커 풀을 종료
    pub fn shutdown(self) {
        // 채널을 닫아서 워커들에게 종료 신호
        drop(self.task_sender);
        drop(self.response_receiver);

        // 모든 워커 스레드가 종료될 때까지 대기
        for (i, handle) in self.worker_threads.into_iter().enumerate() {
            println!("Waiting for worker {} to finish...", i);
            let _ = handle.join();
        }

        println!("All workers shut down");
    }
}

/// 워커 스레드 메인 루프
/// 작업을 받아서 처리하고 응답을 전송
fn worker_thread_main(
    worker_id: usize,
    task_rx: Receiver<GameTask>,
    response_tx: Sender<GameResponse>,
) {
    use crate::room::RoomManager;
    use crate::protocol::{Message, MessageType};

    println!("Worker {} started", worker_id);

    // 이 워커가 관리하는 RoomManager
    // 실제로는 여러 워커가 공유할 수도 있지만 지금은 단순화를 위해 각 워커가 하나씩 가짐
    let mut room_manager = RoomManager::new();

    // 기본 로비 룸 생성
    let lobby_room_id = room_manager.create_room("Lobby".to_string(), 0);
    println!("Worker {}: Created lobby room {}", worker_id, lobby_room_id);

    loop {
        match task_rx.recv() {
            Ok(task) => {
                // 작업 처리
                println!("Worker {} processing task: {:?}", worker_id, task);

                match task {
                    GameTask::SessionMessage { session_id, message } => {
                        // 세션으로부터 메시지를 받음
                        handle_session_message(
                            worker_id,
                            session_id,
                            message,
                            &mut room_manager,
                            &response_tx,
                        );
                    }

                    GameTask::JoinRoom { session_id, room_id } => {
                        // 룸 입장 처리
                        match room_manager.join_room(session_id, room_id) {
                            Ok(()) => {
                                println!(
                                    "Worker {}: Session {} joined room {}",
                                    worker_id, session_id, room_id
                                );

                                // 입장 성공 응답 전송
                                let response_msg = Message::response(
                                    format!("Joined room {}", room_id).into_bytes(),
                                );
                                let _ = response_tx.send(GameResponse::SendMessage {
                                    session_id,
                                    message: response_msg,
                                });
                            }
                            Err(e) => {
                                println!(
                                    "Worker {}: Session {} failed to join room {}: {}",
                                    worker_id, session_id, room_id, e
                                );

                                // 입장 실패 응답
                                let error_msg = Message::response(
                                    format!("Failed to join room: {}", e).into_bytes(),
                                );
                                let _ = response_tx.send(GameResponse::SendMessage {
                                    session_id,
                                    message: error_msg,
                                });
                            }
                        }
                    }

                    GameTask::LeaveRoom { session_id } => {
                        // 룸 퇴장 처리
                        if let Some(room_id) = room_manager.leave_room(session_id) {
                            println!(
                                "Worker {}: Session {} left room {}",
                                worker_id, session_id, room_id
                            );
                        }
                    }

                    GameTask::BroadcastToRoom {
                        room_id,
                        message,
                        exclude_session,
                    } => {
                        // 룸 내 브로드캐스트
                        let members = if let Some(exclude) = exclude_session {
                            room_manager.get_room_members_except(room_id, exclude)
                        } else {
                            room_manager.get_room_members(room_id)
                        };

                        if let Some(session_ids) = members {
                            println!(
                                "Worker {}: Broadcasting to {} members in room {}",
                                worker_id,
                                session_ids.len(),
                                room_id
                            );

                            let _ = response_tx.send(GameResponse::SendMessages {
                                session_ids,
                                message,
                            });
                        }
                    }

                    GameTask::SendToSession { session_id, message } => {
                        // 특정 세션에게 메시지 전송
                        let _ = response_tx.send(GameResponse::SendMessage {
                            session_id,
                            message,
                        });
                    }

                    GameTask::DisconnectSession { session_id } => {
                        // 세션 연결 해제 시 룸에서도 제거
                        room_manager.leave_room(session_id);

                        let _ = response_tx.send(GameResponse::CloseSession { session_id });
                    }
                }
            }
            Err(_) => {
                // 채널이 닫힘 - 종료
                println!("Worker {} shutting down (channel closed)", worker_id);
                break;
            }
        }
    }

    println!("Worker {} exited", worker_id);
}

/// 세션 메시지 처리 핸들러
fn handle_session_message(
    worker_id: usize,
    session_id: usize,
    message: crate::protocol::Message,
    room_manager: &mut crate::room::RoomManager,
    response_tx: &Sender<GameResponse>,
) {
    use crate::protocol::{Message, MessageType};

    match message.msg_type {
        MessageType::Echo => {
            // Echo 메시지/ 그대로 돌려보냄
            println!("Worker {}: Echo from session {}", worker_id, session_id);
            let _ = response_tx.send(GameResponse::SendMessage {
                session_id,
                message: Message::echo(message.payload),
            });
        }

        MessageType::JoinRoom => {
            // JoinRoom 메시지/ payload에서 room_id 파싱
            if message.payload.len() >= 8 {
                let mut room_id_bytes = [0u8; 8];
                room_id_bytes.copy_from_slice(&message.payload[..8]);
                let room_id = u64::from_be_bytes(room_id_bytes);

                // 룸이 없으면 생성
                if room_manager.get_room(room_id).is_none() {
                    room_manager.create_room(format!("Room {}", room_id), 0);
                }

                // 룸 입장 처리
                match room_manager.join_room(session_id, room_id) {
                    Ok(()) => {
                        let response_msg =
                            Message::response(format!("Joined room {}", room_id).into_bytes());
                        let _ = response_tx.send(GameResponse::SendMessage {
                            session_id,
                            message: response_msg,
                        });
                    }
                    Err(e) => {
                        let error_msg =
                            Message::response(format!("Failed to join: {}", e).into_bytes());
                        let _ = response_tx.send(GameResponse::SendMessage {
                            session_id,
                            message: error_msg,
                        });
                    }
                }
            }
        }

        MessageType::LeaveRoom => {
            // LeaveRoom 메시지
            if let Some(room_id) = room_manager.leave_room(session_id) {
                let response_msg =
                    Message::response(format!("Left room {}", room_id).into_bytes());
                let _ = response_tx.send(GameResponse::SendMessage {
                    session_id,
                    message: response_msg,
                });
            }
        }

        MessageType::RoomMessage => {
            // RoomMessage/ 같은 룸에 있는 모든 사람에게 브로드캐스트
            if let Some(room_id) = room_manager.get_session_room(session_id) {
                // 본인 제외하고 브로드캐스트
                if let Some(members) = room_manager.get_room_members_except(room_id, session_id) {
                    println!(
                        "Worker {}: Broadcasting from session {} to {} members in room {}",
                        worker_id,
                        session_id,
                        members.len(),
                        room_id
                    );

                    let _ = response_tx.send(GameResponse::SendMessages {
                        session_ids: members,
                        message: Message::room_message(message.payload),
                    });
                }
            } else {
                // 룸에 속하지 않음
                let error_msg = Message::response(b"Not in any room".to_vec());
                let _ = response_tx.send(GameResponse::SendMessage {
                    session_id,
                    message: error_msg,
                });
            }
        }

        MessageType::Response => {
            // Response 메시지는 서버가 보내는 것이므로 무시
            println!(
                "Worker {}: Ignoring Response message from session {}",
                worker_id, session_id
            );
        }
    }
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

    #[test]
    fn test_worker_pool_creation() {
        let (pool, _task_rx, _response_tx) = WorkerPool::new(4);
        assert_eq!(pool.worker_count(), 4);
        pool.shutdown();
    }
}
