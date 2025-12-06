use bytes::{Buf, BufMut, BytesMut};
use std::io;

/// 메시지 최대 크기 (16MB)
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// 메시지 헤더 크기 (4 bytes for length)
const HEADER_SIZE: usize = 4;

/// 메시지 타입
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    /// Echo 메시지
    Echo = 0x01,
    /// Room 입장 요청
    JoinRoom = 0x02,
    /// Room 퇴장
    LeaveRoom = 0x03,
    /// Room 내 브로드캐스트
    RoomMessage = 0x04,
    /// 서버 응답
    Response = 0x05,
}

impl MessageType {
    pub fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            0x01 => Some(MessageType::Echo),
            0x02 => Some(MessageType::JoinRoom),
            0x03 => Some(MessageType::LeaveRoom),
            0x04 => Some(MessageType::RoomMessage),
            0x05 => Some(MessageType::Response),
            _ => None,
        }
    }
}

/// 프로토콜 메시지
///
/// Wire format:
/// ```
/// ┌──────────────┬──────────┬─────────────────┐
/// │  4 bytes     │  1 byte  │   N bytes       │
/// │  Length (u32)│  Type    │   Payload       │
/// └──────────────┴──────────┴─────────────────┘
/// ```
#[derive(Debug, Clone)]
pub struct Message {
    pub msg_type: MessageType,
    pub payload: Vec<u8>,
}

impl Message {
    /// 새 메시지 생성
    pub fn new(msg_type: MessageType, payload: Vec<u8>) -> Self {
        Self { msg_type, payload }
    }

    /// Echo 메시지 생성
    pub fn echo(data: Vec<u8>) -> Self {
        Self::new(MessageType::Echo, data)
    }

    /// Room 입장 메시지 생성
    pub fn join_room(room_id: u64) -> Self {
        let mut payload = Vec::with_capacity(8);
        payload.extend_from_slice(&room_id.to_be_bytes());
        Self::new(MessageType::JoinRoom, payload)
    }

    /// Room 메시지 생성
    pub fn room_message(data: Vec<u8>) -> Self {
        Self::new(MessageType::RoomMessage, data)
    }

    /// 응답 메시지 생성
    pub fn response(data: Vec<u8>) -> Self {
        Self::new(MessageType::Response, data)
    }

    /// 메시지를 바이트로 인코딩
    pub fn encode(&self) -> BytesMut {
        // Length = 1 byte (type) + payload length
        let total_length = 1 + self.payload.len();

        let mut buf = BytesMut::with_capacity(HEADER_SIZE + total_length);

        // Write length (u32, big-endian)
        buf.put_u32(total_length as u32);

        // Write message type
        buf.put_u8(self.msg_type as u8);

        // Write payload
        buf.put_slice(&self.payload);

        buf
    }

    /// 전체 메시지 크기 (헤더 포함)
    pub fn frame_size(&self) -> usize {
        HEADER_SIZE + 1 + self.payload.len()
    }
}

/// 메시지 프레임 파서
///
/// Length-prefixed 프로토콜 파싱
/// 버퍼에서 완전한 메시지 추출
pub struct FrameParser;

impl FrameParser {
    /// 버퍼에서 메시지를 파싱 시도
    ///
    /// Returns:
    /// - `Ok(Some(Message))`: 완전한 메시지 파싱 성공
    /// - `Ok(None)`: 불완전한 메시지, 더 많은 데이터 필요
    /// - `Err(e)`: 파싱 에러
    pub fn parse(buf: &mut BytesMut) -> io::Result<Option<Message>> {
        // 최소한 헤더가 있는지 확인
        if buf.len() < HEADER_SIZE {
            return Ok(None);
        }

        // Length 읽기 (peek, 실제로 consume하지 않음)
        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&buf[..HEADER_SIZE]);
        let length = u32::from_be_bytes(length_bytes) as usize;

        // 메시지 크기 검증
        if length > MAX_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Message too large: {} bytes", length),
            ));
        }

        // 전체 프레임이 도착했는지 확인
        let frame_size = HEADER_SIZE + length;
        if buf.len() < frame_size {
            // 더 많은 데이터 필요
            return Ok(None);
        }

        // 이제 실제로 데이터를 consume
        buf.advance(HEADER_SIZE);

        // Message type 읽기
        let msg_type_byte = buf.get_u8();
        let msg_type = MessageType::from_u8(msg_type_byte)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid message type: {}", msg_type_byte),
                )
            })?;

        // Payload 읽기
        let payload_len = length - 1; // type 1바이트 제외
        let mut payload = vec![0u8; payload_len];
        buf.copy_to_slice(&mut payload);

        Ok(Some(Message { msg_type, payload }))
    }

    /// 버퍼에 완전한 메시지가 있는지 확인 (peek only)
    pub fn has_complete_frame(buf: &BytesMut) -> bool {
        if buf.len() < HEADER_SIZE {
            return false;
        }

        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&buf[..HEADER_SIZE]);
        let length = u32::from_be_bytes(length_bytes) as usize;

        buf.len() >= HEADER_SIZE + length
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_encode_decode() {
        let msg = Message::echo(b"Hello, World!".to_vec());
        let encoded = msg.encode();

        let mut buf = BytesMut::from(encoded.as_ref());
        let decoded = FrameParser::parse(&mut buf).unwrap().unwrap();

        assert_eq!(decoded.msg_type, MessageType::Echo);
        assert_eq!(decoded.payload, b"Hello, World!");
    }

    #[test]
    fn test_incomplete_message() {
        // 헤더만 있는 경우
        let mut buf = BytesMut::new();
        buf.put_u32(10);

        let result = FrameParser::parse(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_multiple_messages() {
        let msg1 = Message::echo(b"First".to_vec());
        let msg2 = Message::echo(b"Second".to_vec());

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&msg1.encode());
        buf.extend_from_slice(&msg2.encode());

        // 첫 번째 메시지 파싱
        let decoded1 = FrameParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(decoded1.payload, b"First");

        // 두 번째 메시지 파싱
        let decoded2 = FrameParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(decoded2.payload, b"Second");

        // 버퍼 비어있음
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_join_room_message() {
        let room_id = 12345u64;
        let msg = Message::join_room(room_id);

        let encoded = msg.encode();
        let mut buf = BytesMut::from(encoded.as_ref());
        let decoded = FrameParser::parse(&mut buf).unwrap().unwrap();

        assert_eq!(decoded.msg_type, MessageType::JoinRoom);

        // room_id 파싱
        let mut room_bytes = [0u8; 8];
        room_bytes.copy_from_slice(&decoded.payload);
        let parsed_room_id = u64::from_be_bytes(room_bytes);

        assert_eq!(parsed_room_id, room_id);
    }
}
