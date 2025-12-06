use spindle::protocol::Message;
use std::io::{Read, Write};
use std::net::TcpStream;

fn main() -> std::io::Result<()> {
    println!("=== Simple Test Client ===\n");

    // 서버 연결
    let mut stream = TcpStream::connect("127.0.0.1:8080")?;
    println!("Connected to server\n");

    // Echo 메시지 생성
    let messages = vec![
        Message::echo(b"Hello, Spindle!".to_vec()),
        Message::echo(b"This is a test message".to_vec()),
        Message::echo(b"Testing buffer pooling".to_vec()),
    ];

    for (i, msg) in messages.iter().enumerate() {
        println!("Sending message {}: {:?}", i + 1, String::from_utf8_lossy(&msg.payload));

        // 메시지 인코딩 및 전송
        let encoded = msg.encode();
        stream.write_all(&encoded)?;
        stream.flush()?;

        // 응답 대기
        std::thread::sleep(std::time::Duration::from_millis(100));

        // 응답 읽기 (간단히 4KB 버퍼 사용)
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf)?;

        if n > 0 {
            println!("  -> Received {} bytes", n);
            // 실제로는 프로토콜 파서를 사용해야 하지만, 간단히 출력
            println!("  -> Raw response: {:?}\n", &buf[..n]);
        }
    }

    println!("All messages sent successfully!");

    Ok(())
}
