#![cfg(feature = "websocket")]

use std::cell::RefCell;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::rc::Rc;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use curl::easy::{Easy, Easy2, Handler, List, ReadError, WriteError, WsFlags, WsFrame, WsOptions};
use curl::multi::{Easy2Handle, EasyHandle, Multi, Socket, WaitFd};
use curl::Error;

const TIMEOUT: Duration = Duration::from_secs(10);
const KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";

struct Server {
    addr: SocketAddr,
    thread: Option<JoinHandle<()>>,
}

impl Server {
    fn new(run: impl FnOnce(TcpStream) + Send + 'static) -> Self {
        Self::with_http_version("HTTP/1.1", run)
    }

    fn with_http_version(
        version: &'static str,
        run: impl FnOnce(TcpStream) + Send + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let thread = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            drop(listener);
            stream.set_read_timeout(Some(TIMEOUT)).unwrap();
            stream.set_write_timeout(Some(TIMEOUT)).unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                stream.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
                assert!(request.len() <= 64 * 1024);
            }
            let request = String::from_utf8(request).unwrap().to_ascii_lowercase();
            assert!(request.starts_with("get /socket http/1.1\r\n"));
            assert!(request.contains("upgrade: websocket\r\n"));
            assert!(request.contains(&format!(
                "sec-websocket-key: {}\r\n",
                KEY.to_ascii_lowercase()
            )));
            stream
                .write_all(
                    format!(
                        "{version} 101 Switching Protocols\r\n\
                Upgrade: websocket\r\nConnection: Upgrade\r\n\
                Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .unwrap();
            run(stream);
        });
        Self {
            addr,
            thread: Some(thread),
        }
    }

    fn url(&self) -> String {
        format!("ws://{}/socket", self.addr)
    }

    fn headers(&self) -> List {
        let mut headers = List::new();
        headers
            .append(&format!("Sec-WebSocket-Key: {KEY}"))
            .unwrap();
        headers
    }

    fn easy(&self) -> Easy {
        let mut easy = Easy::new();
        easy.url(&self.url()).unwrap();
        easy.proxy("").unwrap();
        easy.http_headers(self.headers()).unwrap();
        easy.timeout(TIMEOUT).unwrap();
        easy.ws_connect_only(true).unwrap();
        easy
    }

    fn easy2<H: Handler>(&self, handler: H) -> Easy2<H> {
        let mut easy = Easy2::new(handler);
        easy.url(&self.url()).unwrap();
        easy.proxy("").unwrap();
        easy.http_headers(self.headers()).unwrap();
        easy.timeout(TIMEOUT).unwrap();
        easy.ws_connect_only(true).unwrap();
        easy
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            // Unblock accept when client setup panics before connecting.
            if thread::panicking() {
                let _ = TcpStream::connect_timeout(&self.addr, Duration::from_secs(1));
                let _ = thread.join();
            } else {
                thread.join().unwrap();
            }
        }
    }
}

struct Empty;
impl Handler for Empty {}

type ReadCallback = dyn FnMut(&mut [u8]) -> Result<usize, ReadError>;
type WriteCallback = dyn FnMut(&[u8]) -> Result<usize, WriteError>;

struct Callbacks {
    read: Box<ReadCallback>,
    write: Box<WriteCallback>,
}

impl Default for Callbacks {
    fn default() -> Self {
        Self {
            read: Box::new(|_| Ok(0)),
            write: Box::new(|data| Ok(data.len())),
        }
    }
}

impl Handler for Callbacks {
    fn read(&mut self, data: &mut [u8]) -> Result<usize, ReadError> {
        (self.read)(data)
    }

    fn write(&mut self, data: &[u8]) -> Result<usize, WriteError> {
        (self.write)(data)
    }
}

trait Client {
    fn socket(&self) -> Socket;
    fn send(&mut self, data: &[u8], size: u64, flags: WsFlags) -> Result<usize, Error>;
    fn recv(&mut self, data: &mut [u8]) -> Result<(usize, WsFrame), Error>;
}

macro_rules! client {
    ($ty:ty) => {
        impl Client for $ty {
            fn socket(&self) -> Socket {
                self.active_socket().unwrap().unwrap()
            }
            fn send(&mut self, data: &[u8], size: u64, flags: WsFlags) -> Result<usize, Error> {
                self.ws_send(data, size, flags)
            }
            fn recv(&mut self, data: &mut [u8]) -> Result<(usize, WsFrame), Error> {
                self.ws_recv(data)
            }
        }
    };
}
client!(Easy);
client!(Easy2<Empty>);
client!(EasyHandle);
client!(Easy2Handle<Empty>);

fn retry<T>(socket: Socket, write: bool, mut op: impl FnMut() -> Result<T, Error>) -> T {
    let deadline = Instant::now() + TIMEOUT;
    let multi = Multi::new();
    loop {
        match op() {
            Ok(value) => return value,
            Err(error) if error.is_again() => {
                assert!(Instant::now() < deadline, "socket readiness timed out");
                let mut fd = WaitFd::new();
                fd.set_fd(socket);
                if write {
                    fd.poll_on_write(true);
                } else {
                    fd.poll_on_read(true);
                }
                multi.wait(&mut [fd], Duration::from_millis(100)).unwrap();
            }
            Err(error) => panic!("WebSocket I/O failed: {}", error),
        }
    }
}

fn send(client: &mut impl Client, mut data: &[u8], mut size: u64, flags: WsFlags) {
    loop {
        let written = retry(client.socket(), true, || client.send(data, size, flags));
        assert!(written <= data.len());
        data = &data[written..];
        if data.is_empty() {
            break;
        }
        assert!(written > 0);
        size = 0;
    }
}

fn receive(client: &mut impl Client, data: &mut [u8]) -> (usize, WsFrame) {
    retry(client.socket(), false, || client.recv(data))
}

fn limit_send_buffer(socket: Socket) {
    // Borrow the live libcurl socket without taking ownership or closing it.
    #[cfg(unix)]
    let socket = unsafe { std::os::fd::BorrowedFd::borrow_raw(socket) };
    #[cfg(windows)]
    let socket = unsafe { std::os::windows::io::BorrowedSocket::borrow_raw(socket) };
    socket2::SockRef::from(&socket)
        .set_send_buffer_size(16 * 1024)
        .unwrap();
}

fn read_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0; 2];
    stream.read_exact(&mut header).unwrap();
    assert_eq!(header[0] & 0x70, 0, "unexpected reserved bits");
    assert_ne!(header[1] & 0x80, 0, "client frames must be masked");
    let size = match header[1] & 0x7f {
        126 => {
            let mut bytes = [0; 2];
            stream.read_exact(&mut bytes).unwrap();
            u16::from_be_bytes(bytes) as usize
        }
        127 => {
            let mut bytes = [0; 8];
            stream.read_exact(&mut bytes).unwrap();
            u64::from_be_bytes(bytes) as usize
        }
        size => size as usize,
    };
    assert!(size <= 16 * 1024 * 1024);
    let mut mask = [0; 4];
    stream.read_exact(&mut mask).unwrap();
    let mut body = vec![0; size];
    stream.read_exact(&mut body).unwrap();
    for (i, byte) in body.iter_mut().enumerate() {
        *byte ^= mask[i % 4];
    }
    (header[0], body)
}

fn frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![opcode];
    match payload.len() {
        0..=125 => out.push(payload.len() as u8),
        126..=65535 => {
            out.push(126);
            out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
        _ => {
            out.push(127);
            out.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
    }
    out.extend_from_slice(payload);
    out
}

fn finish_handshake(multi: &Multi) {
    let deadline = Instant::now() + TIMEOUT;
    while multi.perform().unwrap() > 0 {
        assert!(Instant::now() < deadline);
        multi.wait(&mut [], Duration::from_millis(100)).unwrap();
    }
    let mut completed = false;
    multi.messages(|message| {
        message.result().unwrap().unwrap();
        completed = true;
    });
    assert!(completed);
}

#[test]
fn text_binary_empty_and_close_frames() {
    let payloads = [
        (0x81, b"hello".to_vec(), WsFlags::TEXT),
        (0x82, (0..=255).collect(), WsFlags::BINARY),
        (0x81, Vec::new(), WsFlags::TEXT),
        (0x88, vec![0x03, 0xe8], WsFlags::CLOSE),
    ];
    let expected = payloads.clone();
    let server = Server::new(move |mut stream| {
        for (opcode, payload, _) in expected {
            assert_eq!(read_frame(&mut stream), (opcode, payload.clone()));
            stream.write_all(&frame(opcode, &payload)).unwrap();
        }
    });
    let mut easy = server.easy();
    assert_eq!(easy.active_socket().unwrap(), None);
    assert!(easy.ws_meta().is_none());
    easy.perform().unwrap();
    assert_eq!(easy.response_code().unwrap(), 101);
    for (_, payload, flags) in payloads {
        send(&mut easy, &payload, 0, flags);
        let mut buffer = [0; 512];
        let (count, meta) = receive(&mut easy, &mut buffer);
        assert_eq!(&buffer[..count], payload);
        assert_eq!(meta.flags(), flags);
        assert_eq!(meta.bytes_left(), 0);
        assert_eq!(meta.len(), count);
    }
}

#[test]
fn receive_does_not_drain_the_socket_into_unbounded_prefetch() {
    const PAYLOAD_BYTES: usize = 128 * 1024;
    let wire = frame(0x82, &vec![b'x'; PAYLOAD_BYTES]);
    let wire_len = wire.len();
    let (start_tx, start_rx) = mpsc::channel();
    let server = Server::new(move |mut stream| {
        start_rx.recv_timeout(TIMEOUT).unwrap();
        stream.write_all(&wire).unwrap();
        // Keep the connection open until the client has inspected the socket.
        let _ = stream.read(&mut [0]);
    });
    let mut easy = server.easy();
    easy.perform().unwrap();
    // Borrow libcurl's socket without transferring ownership. Delaying the
    // payload until after the handshake keeps it out of the HTTP read buffer.
    #[cfg(unix)]
    let socket = unsafe { std::os::fd::BorrowedFd::borrow_raw(easy.socket()) };
    #[cfg(windows)]
    let socket = unsafe { std::os::windows::io::BorrowedSocket::borrow_raw(easy.socket()) };
    let socket = socket2::SockRef::from(&socket);
    socket.set_recv_buffer_size(256 * 1024).unwrap();
    let mut raw = vec![std::mem::MaybeUninit::uninit(); wire_len];
    let mut available = || match socket.peek(&mut raw) {
        Ok(count) => count,
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => 0,
        Err(error) => panic!("socket peek failed: {}", error),
    };
    start_tx.send(()).unwrap();
    let deadline = Instant::now() + TIMEOUT;
    while available() < wire_len {
        assert!(
            Instant::now() < deadline,
            "test payload did not reach socket"
        );
        thread::yield_now();
    }
    let mut byte = [0];
    let (count, meta) = easy.ws_recv(&mut byte).unwrap();
    assert_eq!(count, 1);
    assert_eq!(byte, [b'x']);
    assert_eq!(meta.bytes_left(), (PAYLOAD_BYTES - 1) as u64);
    assert!(
        available() >= PAYLOAD_BYTES / 2,
        "a one-byte receive must not prefetch all queued network data"
    );
}

#[test]
fn partial_payload_and_fragmented_message_sends() {
    let server = Server::new(|mut stream| {
        assert_eq!(read_frame(&mut stream), (0x81, b"hello world".to_vec()));
        assert_eq!(read_frame(&mut stream), (0x01, b"first".to_vec()));
        assert_eq!(read_frame(&mut stream), (0x00, b"middle".to_vec()));
        assert_eq!(read_frame(&mut stream), (0x80, b"last".to_vec()));
        stream.write_all(&frame(0x81, b"ok")).unwrap();
    });
    let mut easy = server.easy2(Empty);
    easy.perform().unwrap();
    send(&mut easy, b"hello", 11, WsFlags::TEXT | WsFlags::OFFSET);
    send(&mut easy, b" world", 0, WsFlags::TEXT | WsFlags::OFFSET);
    send(&mut easy, b"first", 0, WsFlags::TEXT | WsFlags::CONT);
    send(&mut easy, b"middle", 0, WsFlags::TEXT | WsFlags::CONT);
    send(&mut easy, b"last", 0, WsFlags::TEXT);
    let mut buffer = [0; 2];
    assert_eq!(receive(&mut easy, &mut buffer).0, 2);
    assert_eq!(&buffer, b"ok");
}

#[test]
fn metadata_peek_and_owned_snapshots() {
    let server = Server::new(|mut stream| {
        stream.write_all(&frame(0x81, b"abcdef")).unwrap();
        assert_eq!(read_frame(&mut stream), (0x88, vec![]));
    });
    let mut easy = server.easy();
    easy.perform().unwrap();
    let (count, peek) = receive(&mut easy, &mut []);
    assert_eq!(count, 0);
    assert_eq!(peek.offset(), 0);
    assert_eq!(peek.bytes_left(), 6);
    let mut buffer = [0; 3];
    let (count, first) = receive(&mut easy, &mut buffer);
    assert_eq!(count, 3);
    assert_eq!(&buffer, b"abc");
    assert_eq!(first.offset(), 0);
    assert_eq!(first.bytes_left(), 3);
    let (count, last) = receive(&mut easy, &mut buffer);
    assert_eq!(count, 3);
    assert_eq!(&buffer, b"def");
    assert_eq!(last.offset(), 3);
    assert_eq!(last.bytes_left(), 0);
    send(&mut easy, &[], 0, WsFlags::CLOSE);
    drop(easy);
    assert_eq!(peek.bytes_left(), 6);
    assert_eq!(first.bytes_left(), 3);
    assert_eq!(last.len(), 3);
}

#[test]
fn fragmented_receive_with_manual_pong() {
    let server = Server::new(|mut stream| {
        stream.write_all(&frame(0x01, b"hel")).unwrap();
        stream.write_all(&frame(0x89, b"alive")).unwrap();
        assert_eq!(read_frame(&mut stream), (0x8a, b"alive".to_vec()));
        stream.write_all(&frame(0x80, b"lo")).unwrap();
    });
    let mut easy = server.easy();
    easy.ws_options(WsOptions::new().no_auto_pong(true))
        .unwrap();
    easy.perform().unwrap();
    let mut buffer = [0; 32];
    let (count, meta) = receive(&mut easy, &mut buffer);
    assert_eq!(meta.flags(), WsFlags::TEXT | WsFlags::CONT);
    assert_eq!(&buffer[..count], b"hel");
    let (count, meta) = receive(&mut easy, &mut buffer);
    assert_eq!(meta.flags(), WsFlags::PING);
    assert_eq!(&buffer[..count], b"alive");
    send(&mut easy, &buffer[..count], 0, WsFlags::PONG);
    let (count, meta) = receive(&mut easy, &mut buffer);
    assert_eq!(meta.flags(), WsFlags::TEXT);
    assert_eq!(&buffer[..count], b"lo");
}

#[test]
fn default_auto_pong_and_option_reset() {
    let server = Server::new(|mut stream| {
        stream.write_all(&frame(0x89, b"ping")).unwrap();
        assert_eq!(read_frame(&mut stream), (0x8a, b"ping".to_vec()));
        stream.write_all(&frame(0x81, b"pong received")).unwrap();
    });
    let mut easy = server.easy();
    easy.ws_options(
        WsOptions::new()
            .no_auto_pong(true)
            .no_auto_pong(false)
            .raw_mode(true)
            .raw_mode(false),
    )
    .unwrap();
    easy.perform().unwrap();
    let mut buffer = [0; 32];
    let (count, meta) = receive(&mut easy, &mut buffer);
    assert_eq!(meta.flags(), WsFlags::TEXT);
    assert_eq!(&buffer[..count], b"pong received");
}

#[test]
fn multi_handle_retains_connection_until_removed() {
    let (done_tx, done_rx) = mpsc::channel();
    let server = Server::new(move |mut stream| {
        assert_eq!(read_frame(&mut stream), (0x81, b"attached".to_vec()));
        stream.write_all(&frame(0x81, b"ok")).unwrap();
        done_rx.recv_timeout(TIMEOUT).unwrap();
    });
    let multi = Multi::new();
    let mut handle = multi.add(server.easy()).unwrap();
    finish_handshake(&multi);
    assert_eq!(handle.response_code().unwrap(), 101);
    assert!(handle.ws_meta().is_none());
    send(&mut handle, b"attached", 0, WsFlags::TEXT);
    let mut buffer = [0; 16];
    assert_eq!(receive(&mut handle, &mut buffer).0, 2);
    let mut easy = multi.remove(handle).unwrap();
    assert!(easy.ws_send(b"detached", 0, WsFlags::TEXT).is_err());
    done_tx.send(()).unwrap();
}

#[test]
#[cfg(feature = "static-curl")]
fn http10_upgrade_retains_easy_connection() {
    let server = Server::with_http_version("HTTP/1.0", |mut stream| {
        assert_eq!(read_frame(&mut stream), (0x81, b"still open".to_vec()));
        stream.write_all(&frame(0x81, b"echo")).unwrap();
        assert_eq!(read_frame(&mut stream), (0x88, Vec::new()));
        stream.write_all(&frame(0x88, b"")).unwrap();
    });
    let mut easy = server.easy();
    easy.perform().unwrap();
    send(&mut easy, b"still open", 0, WsFlags::TEXT);
    let mut buffer = [0; 32];
    let (len, metadata) = receive(&mut easy, &mut buffer);
    assert_eq!(metadata.flags(), WsFlags::TEXT);
    assert_eq!(&buffer[..len], b"echo");
    send(&mut easy, b"", 0, WsFlags::CLOSE);
    assert_eq!(receive(&mut easy, &mut buffer).1.flags(), WsFlags::CLOSE);
}

#[test]
#[cfg(feature = "static-curl")]
fn http10_upgrade_retains_multi_easy2_connection() {
    let server = Server::with_http_version("HTTP/1.0", |mut stream| {
        assert_eq!(read_frame(&mut stream), (0x82, b"still open".to_vec()));
        stream.write_all(&frame(0x82, b"echo")).unwrap();
        assert_eq!(read_frame(&mut stream), (0x88, Vec::new()));
        stream.write_all(&frame(0x88, b"")).unwrap();
    });
    let multi = Multi::new();
    let mut handle = multi.add2(server.easy2(Empty)).unwrap();
    finish_handshake(&multi);
    send(&mut handle, b"still open", 0, WsFlags::BINARY);
    let mut buffer = [0; 32];
    let (len, metadata) = receive(&mut handle, &mut buffer);
    assert_eq!(metadata.flags(), WsFlags::BINARY);
    assert_eq!(&buffer[..len], b"echo");
    send(&mut handle, b"", 0, WsFlags::CLOSE);
    assert_eq!(receive(&mut handle, &mut buffer).1.flags(), WsFlags::CLOSE);
    drop(multi.remove2(handle).unwrap());
}

#[test]
fn multi_easy2_nonblocking_receive_and_large_frame() {
    let payload: Vec<_> = (0..8 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let expected = payload.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let server = Server::new(move |mut stream| {
        ready_rx.recv_timeout(TIMEOUT).unwrap();
        assert_eq!(read_frame(&mut stream), (0x82, expected.clone()));
        stream.write_all(&frame(0x82, &expected)).unwrap();
    });
    let multi = Multi::new();
    let mut handle = multi.add2(server.easy2(Empty)).unwrap();
    finish_handshake(&multi);
    limit_send_buffer(handle.socket());
    assert!(handle.ws_meta().is_none());
    let mut buffer = [0; 16 * 1024];
    assert!(handle.ws_recv(&mut buffer).unwrap_err().is_again());
    let sent = match handle.ws_send(&payload, 0, WsFlags::BINARY) {
        Ok(sent) => sent,
        Err(error) if error.is_again() => 0,
        Err(error) => panic!("{}", error),
    };
    ready_tx.send(()).unwrap();
    assert!(sent < payload.len(), "server must apply send backpressure");
    if sent < payload.len() {
        send(&mut handle, &payload[sent..], 0, WsFlags::BINARY);
    }
    let mut received = Vec::new();
    while received.len() < payload.len() {
        let (count, meta) = receive(&mut handle, &mut buffer);
        assert!(count > 0);
        assert_eq!(meta.flags(), WsFlags::BINARY);
        assert_eq!(meta.offset(), received.len() as u64);
        received.extend_from_slice(&buffer[..count]);
        assert_eq!(meta.bytes_left(), (payload.len() - received.len()) as u64);
    }
    assert_eq!(received, payload);
}

#[test]
fn callback_metadata_is_copied_and_absent_outside_callback() {
    let server = Server::new(|mut stream| {
        stream.write_all(&frame(0x81, b"callback")).unwrap();
    });
    let mut easy = server.easy2(Callbacks::default());
    easy.ws_connect_only(false).unwrap();
    let easy = Rc::new(RefCell::new(easy));
    let weak = Rc::downgrade(&easy);
    let captured = Rc::new(RefCell::new(Vec::new()));
    let output = Rc::clone(&captured);
    easy.borrow_mut().get_mut().write = Box::new(move |data| {
        let meta = weak.upgrade().unwrap().borrow().ws_meta().unwrap();
        output.borrow_mut().push((data.to_vec(), meta));
        Ok(0) // Deliberately stop this otherwise long-lived transfer.
    });
    assert!(easy.borrow().perform().unwrap_err().is_write_error());
    assert!(easy.borrow().ws_meta().is_none());
    drop(easy);
    let captured = captured.borrow();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].0, b"callback");
    assert_eq!(captured[0].1.flags(), WsFlags::TEXT);
}

#[test]
fn raw_callback_receives_wire_bytes_without_metadata() {
    let wire = frame(0x81, b"raw");
    let sent = wire.clone();
    let server = Server::new(move |mut stream| stream.write_all(&sent).unwrap());
    let mut easy = server.easy2(Callbacks::default());
    easy.ws_connect_only(false).unwrap();
    easy.ws_options(WsOptions::new().raw_mode(true)).unwrap();
    let easy = Rc::new(RefCell::new(easy));
    let weak = Rc::downgrade(&easy);
    let captured = Rc::new(RefCell::new(Vec::new()));
    let output = Rc::clone(&captured);
    easy.borrow_mut().get_mut().write = Box::new(move |data| {
        assert!(weak.upgrade().unwrap().borrow().ws_meta().is_none());
        output.borrow_mut().extend_from_slice(data);
        if output.borrow().len() == 5 {
            Ok(0)
        } else {
            Ok(data.len())
        }
    });
    assert!(easy.borrow().perform().unwrap_err().is_write_error());
    assert_eq!(*captured.borrow(), wire);
}

#[test]
fn frame_length_overflow_is_rejected_before_native_call() {
    let mut easy = Easy::new();
    assert_eq!(
        easy.ws_send(&[], u64::MAX, WsFlags::TEXT | WsFlags::OFFSET)
            .unwrap_err()
            .code(),
        curl_sys::CURLE_BAD_FUNCTION_ARGUMENT
    );
    #[cfg(feature = "ws_start_frame_8_16_0")]
    assert_eq!(
        easy.ws_start_frame(WsFlags::TEXT, u64::MAX)
            .unwrap_err()
            .code(),
        curl_sys::CURLE_BAD_FUNCTION_ARGUMENT
    );
}

#[cfg(feature = "ws_start_frame_8_16_0")]
#[test]
fn callback_upload_starts_text_and_empty_frames() {
    let server = Server::new(|mut stream| {
        assert_eq!(read_frame(&mut stream), (0x81, Vec::new()));
        assert_eq!(read_frame(&mut stream), (0x81, b"upload".to_vec()));
        stream.write_all(&frame(0x81, b"ack")).unwrap();
    });
    let mut easy = server.easy2(Callbacks::default());
    easy.ws_connect_only(false).unwrap();
    easy.upload(true).unwrap();
    let easy = Rc::new(RefCell::new(easy));
    let weak = Rc::downgrade(&easy);
    let mut step = 0;
    easy.borrow_mut().get_mut().read = Box::new(move |data| {
        step += 1;
        match step {
            1 => {
                weak.upgrade()
                    .unwrap()
                    .borrow()
                    .ws_start_frame(WsFlags::TEXT, 0)
                    .unwrap();
                Ok(0)
            }
            2 => {
                weak.upgrade()
                    .unwrap()
                    .borrow()
                    .ws_start_frame(WsFlags::TEXT, 6)
                    .unwrap();
                data[..6].copy_from_slice(b"upload");
                Ok(6)
            }
            _ => Err(ReadError::Pause),
        }
    });
    let captured = Rc::new(RefCell::new(Vec::new()));
    let output = Rc::clone(&captured);
    easy.borrow_mut().get_mut().write = Box::new(move |data| {
        output.borrow_mut().extend_from_slice(data);
        Ok(0)
    });
    assert!(easy.borrow().perform().unwrap_err().is_write_error());
    assert_eq!(*captured.borrow(), b"ack");
}
