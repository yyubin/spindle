use crate::protocol::Message;
use crate::session::{RoomId, SessionId};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// LocalTask는 특정 스레드에 고정되어 실행되는 Task
/// !Send Future를 받을 수 있어 스레드 로컬 데이터 접근 가능
pub struct LocalTask {
    future: Pin<Box<dyn Future<Output = ()>>>,
}

impl LocalTask {
    /// 새로운 LocalTask 생성
    /// 고정된 스레드에서만 실행
    pub fn local(future: impl Future<Output = ()> + 'static) -> Self {
        Self {
            future: Box::pin(future),
        }
    }

    /// Task를 폴링
    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.future.as_mut().poll(cx)
    }
}

/// SharedTask는 어떤 스레드에서든 실행될 수 있는 Task
/// Send Future만 받을 수 있어 스레드 간 이동 가능
pub struct SharedTask {
    future: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl SharedTask {
    /// 새로운 SharedTask를 생성
    /// 아무 스레드에서나 실행될 수 있음
    pub fn shared(future: impl Future<Output = ()> + Send + 'static) -> Self {
        Self {
            future: Box::pin(future),
        }
    }

    /// Task를 폴링
    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.future.as_mut().poll(cx)
    }
}

/// 게임 서버 작업 타입
/// I/O 스레드에서 워커 스레드로 전달되는 작업들
#[derive(Debug, Clone)]
pub enum GameTask {
    /// 세션이 메시지를 받았을 때
    SessionMessage {
        session_id: SessionId,
        message: Message,
    },

    /// 세션이 룸에 입장 요청
    JoinRoom {
        session_id: SessionId,
        room_id: RoomId,
    },

    /// 세션이 룸에서 퇴장
    LeaveRoom {
        session_id: SessionId,
    },

    /// 룸 내 브로드캐스트 메시지
    BroadcastToRoom {
        room_id: RoomId,
        message: Message,
        exclude_session: Option<SessionId>, // 특정 세션 제외 (본인 제외용)
    },

    /// 특정 세션에게 메시지 전송
    SendToSession {
        session_id: SessionId,
        message: Message,
    },

    /// 세션 연결 해제
    DisconnectSession {
        session_id: SessionId,
    },
}

/// 워커 스레드에서 I/O 스레드로 보내는 응답
/// 워커가 처리한 결과를 I/O 스레드에 알려주기
#[derive(Debug, Clone)]
pub enum GameResponse {
    /// 세션에게 메시지 전송 요청
    SendMessage {
        session_id: SessionId,
        message: Message,
    },

    /// 여러 세션에게 메시지 전송 (브로드캐스트)
    SendMessages {
        session_ids: Vec<SessionId>,
        message: Message,
    },

    /// 세션 연결 종료 요청
    CloseSession {
        session_id: SessionId,
    },
}
