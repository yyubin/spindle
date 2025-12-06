use crate::buffer_pool::BufferPool;
use crate::session::{Session, SessionId};
use crate::task::{GameResponse, GameTask};
use crate::worker::WorkerPool;
use crossbeam_channel::{Receiver, Sender};
use mio::{Events, Interest, Poll, Token};
use mio::net::TcpListener;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// 서버 리스너 토큰 (Token(0))
const SERVER_TOKEN: Token = Token(0);

/// 세션 토큰 시작 번호
const SESSION_TOKEN_START: usize = 1;

/// I/O Driver는 Reactor 패턴을 구현합니다.
/// mio Poll을 사용해 NIO 방식으로 이벤트를 수집합니다.
pub struct IoDriver {
    /// mio Poll (epoll/kqueue wrapper)
    poll: Poll,
    /// 이벤트 버퍼
    events: Events,
    /// TCP 서버 리스너
    listener: TcpListener,
    /// 활성 세션들 (SessionId -> Session)
    sessions: HashMap<SessionId, Session>,
    /// 다음 세션 ID
    next_session_id: SessionId,
    /// 버퍼 풀
    buffer_pool: Arc<BufferPool>,
    /// 워커 스레드 풀
    worker_pool: WorkerPool,
    /// I/O -> Worker 채널 (사용하지 않지만 drop 방지를 위해 보관)
    _task_rx: Receiver<GameTask>,
    /// Worker -> I/O 응답 채널
    response_tx: Sender<GameResponse>,
}

impl IoDriver {
    /// 새로운 IoDriver를 생성합니다.
    ///
    /// # Arguments
    /// * `bind_addr` - 바인딩할 주소 (예: "127.0.0.1:8080")
    /// * `worker_count` - 워커 스레드 수 (기본값: CPU 코어 수)
    pub fn new(bind_addr: SocketAddr) -> io::Result<Self> {
        let poll = Poll::new()?;
        let events = Events::with_capacity(1024);

        // TCP 리스너 생성 및 등록
        let mut listener = TcpListener::bind(bind_addr)?;
        poll.registry()
            .register(&mut listener, SERVER_TOKEN, Interest::READABLE)?;

        println!("TCP Server listening on {}", bind_addr);

        let buffer_pool = BufferPool::with_defaults();

        // 워커 풀 생성 (CPU 코어 수만큼)
        let worker_count = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        let (worker_pool, task_rx, response_tx) = WorkerPool::new(worker_count);
        println!("Created worker pool with {} workers", worker_count);

        Ok(Self {
            poll,
            events,
            listener,
            sessions: HashMap::new(),
            next_session_id: SESSION_TOKEN_START,
            buffer_pool,
            worker_pool,
            _task_rx: task_rx,
            response_tx,
        })
    }

    /// Reactor loop를 실행합니다.
    /// 무한 루프 안에서 I/O readiness events를 수집하고 처리합니다.
    pub fn run(&mut self) -> io::Result<()> {
        println!("IoDriver started, entering reactor loop...");

        loop {
            // I/O readiness events를 대기합니다
            // timeout을 설정하여 주기적으로 다른 작업도 처리할 수 있게 합니다
            self.poll
                .poll(&mut self.events, Some(Duration::from_millis(100)))?;

            // 수집된 events를 처리합니다
            // borrow checker를 위해 이벤트를 복사
            let events_to_process: Vec<_> = self
                .events
                .iter()
                .map(|e| (e.token(), e.is_readable(), e.is_writable(), e.is_error(), e.is_read_closed(), e.is_write_closed()))
                .collect();

            for (token, readable, writable, error, read_closed, write_closed) in events_to_process {
                self.handle_event_data(token, readable, writable, error, read_closed, write_closed)?;
            }

            // 워커로부터 응답 처리
            self.process_worker_responses();

            // 주기적으로 통계 출력 (나중에 제거 가능)
            self.print_stats();
        }
    }

    /// 개별 event를 처리합니다.
    fn handle_event_data(
        &mut self,
        token: Token,
        readable: bool,
        writable: bool,
        error: bool,
        read_closed: bool,
        write_closed: bool,
    ) -> io::Result<()> {
        match token {
            SERVER_TOKEN => {
                // 새로운 연결 수락
                self.accept_connection()?;
            }
            token => {
                // 세션 이벤트 처리
                let session_id = token.0;
                self.handle_session_event(
                    session_id,
                    readable,
                    writable,
                    error,
                    read_closed,
                    write_closed,
                )?;
            }
        }

        Ok(())
    }

    /// 새로운 클라이언트 연결을 수락합니다.
    fn accept_connection(&mut self) -> io::Result<()> {
        loop {
            match self.listener.accept() {
                Ok((mut socket, peer_addr)) => {
                    let session_id = self.next_session_id;
                    self.next_session_id += 1;

                    println!(
                        "New connection accepted: session_id={}, peer={}",
                        session_id, peer_addr
                    );

                    // mio Poll에 소켓 등록
                    let token = Token(session_id);
                    self.poll.registry().register(
                        &mut socket,
                        token,
                        Interest::READABLE | Interest::WRITABLE,
                    )?;

                    // 세션 생성 및 저장
                    let session = Session::new(
                        session_id,
                        socket,
                        peer_addr,
                        self.buffer_pool.clone(),
                    );
                    self.sessions.insert(session_id, session);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // 더 이상 수락할 연결이 없음
                    break;
                }
                Err(e) => {
                    eprintln!("Error accepting connection: {}", e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// 세션 이벤트를 처리합니다.
    fn handle_session_event(
        &mut self,
        session_id: SessionId,
        readable: bool,
        writable: bool,
        error: bool,
        read_closed: bool,
        write_closed: bool,
    ) -> io::Result<()> {
        // 세션을 임시로 가져옴 (borrow checker 회피)
        let mut session = match self.sessions.remove(&session_id) {
            Some(s) => s,
            None => {
                // 세션이 이미 제거됨
                return Ok(());
            }
        };

        let mut should_close = false;

        // Readable 이벤트
        if readable {
            match session.read_from_socket() {
                Ok(0) => {
                    // 연결 종료
                    println!("Session {} closed by peer", session_id);
                    should_close = true;
                }
                Ok(n) => {
                    println!("Session {} read {} bytes", session_id, n);

                    // 메시지 파싱 후 워커로 전달
                    while let Some(msg) = session.parse_message()? {
                        println!(
                            "Session {} received message: type={:?}, payload_len={}",
                            session_id,
                            msg.msg_type,
                            msg.payload.len()
                        );

                        // 워커로 작업 전달
                        let task = GameTask::SessionMessage {
                            session_id,
                            message: msg,
                        };

                        if let Err(e) = self.worker_pool.send_task(task) {
                            eprintln!("Failed to send task to worker: {}", e);
                            should_close = true;
                            break;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error reading from session {}: {}", session_id, e);
                    should_close = true;
                }
            }
        }

        // Writable 이벤트
        if writable && session.has_pending_writes() {
            match session.flush_to_socket() {
                Ok(n) if n > 0 => {
                    println!("Session {} wrote {} bytes", session_id, n);
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Error writing to session {}: {}", session_id, e);
                    should_close = true;
                }
            }
        }

        // 에러 이벤트
        if error || read_closed || write_closed {
            println!("Session {} error or closed event", session_id);
            should_close = true;
        }

        // 세션 복원 또는 제거
        if should_close || session.is_closed() {
            println!("Closing session {}", session_id);
            session.close();
            // 세션은 drop됨 (버퍼 자동 반환)
        } else {
            // 세션을 다시 저장
            self.sessions.insert(session_id, session);
        }

        Ok(())
    }

    /// 워커로부터 응답 처리
    fn process_worker_responses(&mut self) {
        // Non-blocking으로 모든 응답 처리
        loop {
            match self.worker_pool.try_recv_response() {
                Ok(response) => {
                    match response {
                        GameResponse::SendMessage { session_id, message } => {
                            // 특정 세션에게 메시지 전송
                            if let Some(session) = self.sessions.get_mut(&session_id) {
                                session.enqueue_message(message);

                                // 즉시 flush 시도
                                let _ = session.flush_to_socket();
                            }
                        }

                        GameResponse::SendMessages { session_ids, message } => {
                            // 여러 세션에게 메시지 전송 (브로드캐스트)
                            for session_id in session_ids {
                                if let Some(session) = self.sessions.get_mut(&session_id) {
                                    session.enqueue_message(message.clone());

                                    // 즉시 flush 시도
                                    let _ = session.flush_to_socket();
                                }
                            }
                        }

                        GameResponse::CloseSession { session_id } => {
                            // 세션 종료
                            if let Some(mut session) = self.sessions.remove(&session_id) {
                                println!("Closing session {} by worker request", session_id);
                                session.close();
                            }
                        }
                    }
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    // 더 이상 응답이 없음
                    break;
                }
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    // 채널이 닫힘 (워커가 종료됨)
                    eprintln!("Worker response channel disconnected");
                    break;
                }
            }
        }
    }

    /// 통계 출력 (디버그용)
    fn print_stats(&self) {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};

        static LAST_PRINT: AtomicU64 = AtomicU64::new(0);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let last = LAST_PRINT.load(Ordering::Relaxed);

        if now - last >= 10 {
            LAST_PRINT.store(now, Ordering::Relaxed);
            println!(
                "IoDriver stats: sessions={}, buffer_pool={:?}",
                self.sessions.len(),
                self.buffer_pool.stats()
            );
        }
    }

    /// Poll에 대한 참조 반환
    pub fn poll(&self) -> &Poll {
        &self.poll
    }

    /// 활성 세션 수 조회
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// 버퍼 풀 참조
    pub fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }
}
