use crate::buffer_pool::BufferPool;
use crate::protocol::{FrameParser, Message};
use bytes::BytesMut;
use mio::net::TcpStream;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;

/// 세션 ID 타입
pub type SessionId = usize;

/// Room ID 타입
pub type RoomId = u64;

/// 세션 상태
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// 연결됨, 활성 상태
    Active,
    /// 읽기 종료됨
    ReadClosed,
    /// 쓰기 종료됨
    WriteClosed,
    /// 완전히 종료됨
    Closed,
}

/// 클라이언트 세션
/// TCP 연결을 나타내기 / 읽기/쓰기 버퍼와 상태 관리
pub struct Session {
    /// 세션 고유 ID
    id: SessionId,
    /// TCP 소켓
    socket: TcpStream,
    /// 클라이언트 주소
    peer_addr: SocketAddr,
    /// 읽기 버퍼 (소켓에서 읽은 원시 바이트)
    read_buf: BytesMut,
    /// 쓰기 버퍼 (클라이언트로 전송할 바이트)
    write_buf: BytesMut,
    /// 버퍼 풀 (반환용)
    buffer_pool: Arc<BufferPool>,
    /// 현재 상태
    state: SessionState,
    /// 현재 속한 Room ID
    room_id: Option<RoomId>,
}

impl Session {
    /// 새로운 세션 생성
    pub fn new(
        id: SessionId,
        socket: TcpStream,
        peer_addr: SocketAddr,
        buffer_pool: Arc<BufferPool>,
    ) -> Self {
        let read_buf = buffer_pool.acquire();
        let write_buf = buffer_pool.acquire();

        Self {
            id,
            socket,
            peer_addr,
            read_buf,
            write_buf,
            buffer_pool,
            state: SessionState::Active,
            room_id: None,
        }
    }

    /// 세션 ID 조회
    pub fn id(&self) -> SessionId {
        self.id
    }

    /// Peer 주소 조회
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// 현재 상태 조회
    pub fn state(&self) -> SessionState {
        self.state
    }

    /// Room ID 조회
    pub fn room_id(&self) -> Option<RoomId> {
        self.room_id
    }

    /// Room ID 설정
    pub fn set_room_id(&mut self, room_id: Option<RoomId>) {
        self.room_id = room_id;
    }

    /// 소켓에서 데이터 읽기
    /// Returns: 읽은 바이트 수 (0이면 연결 종료)
    pub fn read_from_socket(&mut self) -> io::Result<usize> {
        // 버퍼에 공간 확보
        if self.read_buf.capacity() - self.read_buf.len() < 1024 {
            self.read_buf.reserve(4096);
        }

        // 읽기를 위한 임시 버퍼
        let mut temp_buf = [0u8; 4096];

        match self.socket.read(&mut temp_buf) {
            Ok(0) => {
                // 연결 종료
                self.state = SessionState::ReadClosed;
                Ok(0)
            }
            Ok(n) => {
                // 데이터를 read_buf에 추가
                self.read_buf.extend_from_slice(&temp_buf[..n]);
                Ok(n)
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // Non-blocking이므로 WouldBlock은 정상
                Ok(0)
            }
            Err(e) => Err(e),
        }
    }

    /// 읽기 버퍼에서 메시지 파싱
    /// 완전한 메시지가 있으면 파싱하여 반환
    pub fn parse_message(&mut self) -> io::Result<Option<Message>> {
        FrameParser::parse(&mut self.read_buf)
    }

    /// 메시지를 쓰기 버퍼에 추가
    pub fn enqueue_message(&mut self, msg: Message) {
        let encoded = msg.encode();
        self.write_buf.extend_from_slice(&encoded);
    }

    /// 쓰기 버퍼의 데이터를 소켓으로 전송
    /// Returns: 전송한 바이트 수
    pub fn flush_to_socket(&mut self) -> io::Result<usize> {
        if self.write_buf.is_empty() {
            return Ok(0);
        }

        match self.socket.write(&self.write_buf) {
            Ok(n) => {
                // 전송한 만큼 버퍼에서 제거
                let _ = self.write_buf.split_to(n);
                Ok(n)
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // Non-blocking, 나중에 다시 시도
                Ok(0)
            }
            Err(e) => Err(e),
        }
    }

    /// 쓰기 버퍼에 대기 중인 데이터가 있는지 확인
    pub fn has_pending_writes(&self) -> bool {
        !self.write_buf.is_empty()
    }

    /// 읽기 버퍼 크기
    pub fn read_buffer_len(&self) -> usize {
        self.read_buf.len()
    }

    /// 쓰기 버퍼 크기
    pub fn write_buffer_len(&self) -> usize {
        self.write_buf.len()
    }

    /// 소켓 참조 조회 (mio 등록용)
    pub fn socket(&self) -> &TcpStream {
        &self.socket
    }

    /// 소켓 mutable 참조
    pub fn socket_mut(&mut self) -> &mut TcpStream {
        &mut self.socket
    }

    /// 세션 종료
    pub fn close(&mut self) {
        self.state = SessionState::Closed;
        let _ = self.socket.shutdown(std::net::Shutdown::Both);
    }

    /// 세션이 닫혔는지 확인
    pub fn is_closed(&self) -> bool {
        self.state == SessionState::Closed
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        println!("Session {} dropped, returning buffers to pool", self.id);

        // 버퍼를 풀로 반환
        let read_buf = std::mem::replace(&mut self.read_buf, BytesMut::new());
        let write_buf = std::mem::replace(&mut self.write_buf, BytesMut::new());

        self.buffer_pool.release(read_buf);
        self.buffer_pool.release(write_buf);
    }
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("id", &self.id)
            .field("peer_addr", &self.peer_addr)
            .field("state", &self.state)
            .field("room_id", &self.room_id)
            .field("read_buf_len", &self.read_buf.len())
            .field("write_buf_len", &self.write_buf.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_state() {
        // 실제 소켓 없이는 테스트하기 어려우므로
        // 통합 테스트에서 검증
    }
}
