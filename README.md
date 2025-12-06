# Spindle

Rust 멀티스레드 게임 서버

## 특징

- I/O 스레드 (mio) + 워커 스레드 풀 분리
- Room 기반 세션 관리 및 브로드캐스트
- 버퍼 풀링 및 crossbeam-channel 통신

## 실행

```bash
# 서버 실행 (127.0.0.1:8080)
cargo run

# 채팅 클라이언트
cargo run --example chat_client

# Echo 클라이언트
cargo run --example echo_client
```

## 채팅 명령어

- `/join <room_id>` - 룸 입장
- `/leave` - 룸 퇴장
- `/quit` - 종료
- 일반 텍스트 - 룸 메시지 전송

## 프로토콜

```
[4B Length][1B Type][N bytes Payload]
```

**메시지 타입**
- `0x01` Echo
- `0x02` JoinRoom (payload: u64 room_id)
- `0x03` LeaveRoom
- `0x04` RoomMessage
- `0x05` Response
