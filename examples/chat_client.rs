/// 간단한 채팅 클라이언트 예제
///
/// 사용법:
/// 1. 서버 실행: cargo run
/// 2. 클라이언트 실행: cargo run --example chat_client
///
/// 명령어:
/// - /join <room_id>: 룸 입장
/// - /leave: 룸 퇴장
/// - 그 외: 룸 메시지 전송

use std::io::{self, BufRead, Read, Write};
use std::net::TcpStream;

const SERVER_ADDR: &str = "127.0.0.1:8080";

/// 메시지 타입
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum MessageType {
    Echo = 0x01,
    JoinRoom = 0x02,
    LeaveRoom = 0x03,
    RoomMessage = 0x04,
    Response = 0x05,
}

/// 메시지 인코딩
fn encode_message(msg_type: MessageType, payload: &[u8]) -> Vec<u8> {
    let total_length = 1 + payload.len();
    let mut buf = Vec::with_capacity(4 + total_length);

    // Length (u32, big-endian)
    buf.extend_from_slice(&(total_length as u32).to_be_bytes());

    // Message type
    buf.push(msg_type as u8);

    // Payload
    buf.extend_from_slice(payload);

    buf
}

/// 메시지 디코딩
fn decode_message(data: &[u8]) -> Option<(MessageType, Vec<u8>)> {
    if data.len() < 5 {
        return None;
    }

    let mut length_bytes = [0u8; 4];
    length_bytes.copy_from_slice(&data[..4]);
    let length = u32::from_be_bytes(length_bytes) as usize;

    if data.len() < 4 + length {
        return None;
    }

    let msg_type_byte = data[4];
    let msg_type = match msg_type_byte {
        0x01 => MessageType::Echo,
        0x02 => MessageType::JoinRoom,
        0x03 => MessageType::LeaveRoom,
        0x04 => MessageType::RoomMessage,
        0x05 => MessageType::Response,
        _ => return None,
    };

    let payload = data[5..4 + length].to_vec();

    Some((msg_type, payload))
}

fn main() -> io::Result<()> {
    println!("=== Spindle Chat Client ===");
    println!("Connecting to {}...", SERVER_ADDR);

    let mut stream = TcpStream::connect(SERVER_ADDR)?;
    stream.set_nonblocking(true)?;

    println!("Connected!");
    println!("\nCommands:");
    println!("  /join <room_id>  - Join a room");
    println!("  /leave           - Leave current room");
    println!("  <message>        - Send message to room\n");

    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();

    let mut read_buf = vec![0u8; 4096];
    let mut accumulated_buf = Vec::new();

    loop {
        // 서버로부터 메시지 읽기 (non-blocking)
        match stream.read(&mut read_buf) {
            Ok(0) => {
                println!("Server closed connection");
                break;
            }
            Ok(n) => {
                accumulated_buf.extend_from_slice(&read_buf[..n]);

                // 메시지 파싱 시도
                while accumulated_buf.len() >= 4 {
                    let mut length_bytes = [0u8; 4];
                    length_bytes.copy_from_slice(&accumulated_buf[..4]);
                    let length = u32::from_be_bytes(length_bytes) as usize;

                    let frame_size = 4 + length;
                    if accumulated_buf.len() < frame_size {
                        break;
                    }

                    let frame = &accumulated_buf[..frame_size];
                    if let Some((msg_type, payload)) = decode_message(frame) {
                        handle_server_message(msg_type, &payload);
                    }

                    accumulated_buf.drain(..frame_size);
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // 데이터 없음, 계속 진행
            }
            Err(e) => {
                eprintln!("Error reading from server: {}", e);
                break;
            }
        }

        // 사용자 입력 처리 (non-blocking)
        if let Some(Ok(line)) = lines.next() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            if line.starts_with('/') {
                // 명령어 처리
                let parts: Vec<&str> = line.split_whitespace().collect();
                match parts[0] {
                    "/join" => {
                        if parts.len() < 2 {
                            println!("Usage: /join <room_id>");
                            continue;
                        }

                        if let Ok(room_id) = parts[1].parse::<u64>() {
                            let payload = room_id.to_be_bytes().to_vec();
                            let msg = encode_message(MessageType::JoinRoom, &payload);
                            stream.write_all(&msg)?;
                            println!("Joining room {}...", room_id);
                        } else {
                            println!("Invalid room_id");
                        }
                    }

                    "/leave" => {
                        let msg = encode_message(MessageType::LeaveRoom, &[]);
                        stream.write_all(&msg)?;
                        println!("Leaving room...");
                    }

                    "/quit" | "/exit" => {
                        println!("Goodbye!");
                        break;
                    }

                    _ => {
                        println!("Unknown command: {}", parts[0]);
                    }
                }
            } else {
                // 일반 메시지 - 룸에 전송
                let payload = line.as_bytes().to_vec();
                let msg = encode_message(MessageType::RoomMessage, &payload);
                stream.write_all(&msg)?;
            }
        }
        
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    Ok(())
}

fn handle_server_message(msg_type: MessageType, payload: &[u8]) {
    match msg_type {
        MessageType::Echo => {
            let text = String::from_utf8_lossy(payload);
            println!("[Echo] {}", text);
        }

        MessageType::Response => {
            let text = String::from_utf8_lossy(payload);
            println!("[Server] {}", text);
        }

        MessageType::RoomMessage => {
            let text = String::from_utf8_lossy(payload);
            println!("[Room] {}", text);
        }

        _ => {
            println!("[Unknown] {:?} - {} bytes", msg_type, payload.len());
        }
    }
}
