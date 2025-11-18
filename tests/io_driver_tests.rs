use spindle::io_driver::IoDriver;
use mio::{Interest, Token};
use mio::net::TcpListener;
use std::net::SocketAddr;
use std::time::Duration;

#[test]
fn test_io_driver_creation() {
    let driver = IoDriver::new();
    assert!(driver.is_ok(), "IoDriver should be created successfully");
}

#[test]
fn test_io_driver_default() {
    let driver = IoDriver::default();
    // Default trait이 정상적으로 동작하는지 확인
    let _poll = driver.poll();
    // Poll이 정상적으로 반환되면 성공
}

#[test]
fn test_io_driver_poll_access() {
    let driver = IoDriver::new().unwrap();
    let _poll = driver.poll();
    // Poll이 정상적으로 반환되면 성공
}

#[test]
fn test_io_driver_with_tcp_listener() -> std::io::Result<()> {
    let driver = IoDriver::new()?;

    // TCP 리스너 생성
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut listener = TcpListener::bind(addr)?;

    // Poll에 리스너 등록
    driver.poll().registry().register(
        &mut listener,
        Token(0),
        Interest::READABLE,
    )?;

    // 등록이 성공했는지 확인 (에러가 없으면 성공)
    Ok(())
}

#[test]
fn test_io_driver_multiple_sources() -> std::io::Result<()> {
    let driver = IoDriver::new()?;

    // 여러 개의 TCP 리스너 생성 및 등록
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut listener1 = TcpListener::bind(addr1)?;

    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut listener2 = TcpListener::bind(addr2)?;

    // 여러 소스를 Poll에 등록
    driver.poll().registry().register(
        &mut listener1,
        Token(0),
        Interest::READABLE,
    )?;

    driver.poll().registry().register(
        &mut listener2,
        Token(1),
        Interest::READABLE,
    )?;

    Ok(())
}

#[test]
fn test_io_driver_poll_timeout() -> std::io::Result<()> {
    use mio::{Events, Poll};

    // Poll을 직접 생성해서 timeout 동작 확인
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(128);

    let start = std::time::Instant::now();

    // 100ms timeout으로 poll (이벤트 없음)
    poll.poll(&mut events, Some(Duration::from_millis(100)))?;

    let elapsed = start.elapsed();

    // timeout이 대략적으로 맞는지 확인 (50ms ~ 200ms 사이)
    assert!(elapsed >= Duration::from_millis(50));
    assert!(elapsed <= Duration::from_millis(200));

    Ok(())
}
