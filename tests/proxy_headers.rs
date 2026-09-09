use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use curl::easy::{Easy, Easy2, Handler, List};

const TIMEOUT: Duration = Duration::from_secs(10);

struct Tunnel {
    address: SocketAddr,
    thread: Option<JoinHandle<(String, String)>>,
}

impl Tunnel {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let thread = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream.set_read_timeout(Some(TIMEOUT)).unwrap();
            stream.set_write_timeout(Some(TIMEOUT)).unwrap();
            let connect = read_headers(&mut stream);
            stream
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .unwrap();
            let request = read_headers(&mut stream);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();
            (connect, request)
        });
        Self {
            address,
            thread: Some(thread),
        }
    }

    fn proxy(&self) -> String {
        format!("http://{}", self.address)
    }

    fn finish(mut self) -> (String, String) {
        self.thread.take().unwrap().join().unwrap()
    }
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            // A setup panic must not leave accept waiting forever.
            let _ = TcpStream::connect_timeout(&self.address, Duration::from_secs(1));
            let _ = thread.join();
        }
    }
}

fn read_headers(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        stream.read_exact(&mut byte).unwrap();
        bytes.push(byte[0]);
        assert!(bytes.len() <= 64 * 1024);
    }
    String::from_utf8(bytes).unwrap().to_ascii_lowercase()
}

fn headers(value: &str) -> List {
    let mut list = List::new();
    list.append(value).unwrap();
    list
}

struct Empty;
impl Handler for Empty {}

#[test]
fn easy2_proxy_headers_are_owned_replaced_and_isolated_from_origin() {
    let tunnel = Tunnel::new();
    let mut easy = Easy2::new(Empty);
    easy.url("http://origin.invalid/socket").unwrap();
    easy.proxy(&tunnel.proxy()).unwrap();
    easy.noproxy("").unwrap();
    easy.http_proxy_tunnel(true).unwrap();
    easy.timeout(TIMEOUT).unwrap();
    easy.separate_proxy_headers(true).unwrap();
    easy.http_headers(headers("X-Origin-Secret: origin-only"))
        .unwrap();
    easy.proxy_headers(headers("Proxy-Authorization: Bearer obsolete"))
        .unwrap();
    easy.proxy_headers(headers("Proxy-Authorization: Bearer proxy-only"))
        .unwrap();
    easy.perform().unwrap();
    let (connect, request) = tunnel.finish();
    assert!(connect.starts_with("connect origin.invalid:80 http/1.1\r\n"));
    assert!(connect.contains("proxy-authorization: bearer proxy-only\r\n"));
    assert!(!connect.contains("obsolete"));
    assert!(!connect.contains("x-origin-secret"));
    assert!(request.starts_with("get /socket http/1.1\r\n"));
    assert!(request.contains("x-origin-secret: origin-only\r\n"));
    assert!(!request.contains("proxy-authorization"));
}

#[test]
fn easy_proxy_headers_can_be_cleared_and_reset() {
    let mut easy = Easy::new();
    easy.proxy_headers(headers("X-Proxy-Secret: before-reset"))
        .unwrap();
    easy.reset();
    for clear in [false, true] {
        let tunnel = Tunnel::new();
        easy.url("http://origin.invalid/socket").unwrap();
        easy.proxy(&tunnel.proxy()).unwrap();
        easy.noproxy("").unwrap();
        easy.http_proxy_tunnel(true).unwrap();
        easy.timeout(TIMEOUT).unwrap();
        easy.separate_proxy_headers(true).unwrap();
        if clear {
            easy.proxy_headers(headers("X-Proxy-Secret: before-clear"))
                .unwrap();
            easy.proxy_headers(List::new()).unwrap();
        }
        easy.perform().unwrap();
        let (connect, request) = tunnel.finish();
        assert!(!connect.contains("x-proxy-secret"));
        assert!(!request.contains("x-proxy-secret"));
    }
}
