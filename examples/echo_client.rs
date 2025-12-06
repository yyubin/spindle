/// 간단한 Echo 클라이언트 예제
///
/// 사용법:
/// 1. 서버 실행: cargo run
/// 2. Echo 클라이언트 실행: cargo run --example echo_client

use std::io::{self, Read, Write};
use std::net::TcpStream;

const SERVER_ADDR: &str = "127.0.0.1:8080";

/// Echo 메시지 인코딩
fn encode_echo_message(text: &str) -> Vec<u8> {
    let payload = text.as_bytes();
    let total_length = 1 + payload.len();
    let mut buf = Vec::with_capacity(4 + total_length);

    // Length (u32, big-endian)
    buf.extend_from_slice(&(total_length as u32).to_be_bytes());

    // Message type (Echo = 0x01)
    buf.push(0x01);

    // Payload
    buf.extend_from_slice(payload);

    buf
}

fn main() -> io::Result<()> {
    println!("=== Spindle Echo Client ===");
    println!("Connecting to {}...", SERVER_ADDR);

    let mut stream = TcpStream::connect(SERVER_ADDR)?;

    println!("Connected!");
    println!("Type messages to echo (Ctrl+C to exit)\n");

    let stdin = io::stdin();
    let mut line = String::new();

    loop {
        line.clear();
        print!("> ");
        io::stdout().flush()?;

        stdin.read_line(&mut line)?;
        let text = line.trim();

        if text.is_empty() {
            continue;
        }

        // Echo 메시지 전송
        let msg = encode_echo_message(text);
        stream.write_all(&msg)?;
        println!("Sent: {}", text);

        // 응답 받기
        let mut header = [0u8; 4];
        stream.read_exact(&mut header)?;
        let length = u32::from_be_bytes(header) as usize;

        let mut response = vec![0u8; length];
        stream.read_exact(&mut response)?;

        // Message type (1 byte)
        let msg_type = response[0];
        let payload = &response[1..];

        let response_text = String::from_utf8_lossy(payload);
        println!("Received (type={}): {}\n", msg_type, response_text);
    }
}
