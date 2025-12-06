use spindle::io_driver::IoDriver;
use std::net::SocketAddr;

fn main() -> std::io::Result<()> {
    println!("=== Spindle Game Server ===\n");

    // 서버 주소 설정
    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

    // IoDriver 생성 및 실행
    let mut driver = IoDriver::new(addr)?;

    println!("Server starting...");
    println!("Connect with: telnet 127.0.0.1 8080");
    println!("Or use a custom client\n");

    // Reactor loop 실행
    driver.run()
}
