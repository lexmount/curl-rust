use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use curl::easy::{Easy, Easy2, Handler, InfoType};
use curl::multi::Multi;
use curl::Version;

const TIMEOUT: Duration = Duration::from_secs(10);
const EMPTY: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";

fn supported() -> bool {
    Version::get().version_num() >= 0x075000
}

fn read_head(stream: &mut TcpStream) -> Vec<u8> {
    stream.set_read_timeout(Some(TIMEOUT)).unwrap();
    let mut head = Vec::new();
    let mut byte = [0];
    while !head.ends_with(b"\r\n\r\n") {
        if stream.read(&mut byte).unwrap() == 0 {
            break;
        }
        head.push(byte[0]);
        assert!(head.len() < 64 * 1024);
    }
    head
}

fn keepalive_server(responses: Vec<&'static [u8]>) -> (SocketAddr, JoinHandle<Vec<Vec<u8>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_write_timeout(Some(TIMEOUT)).unwrap();
        responses
            .into_iter()
            .map(|response| {
                let head = read_head(&mut stream);
                assert!(head.ends_with(b"\r\n\r\n"));
                stream.write_all(response).unwrap();
                head
            })
            .collect()
    });
    (address, server)
}

fn configure<H>(easy: &mut Easy2<H>, address: SocketAddr) {
    easy.url(&format!("http://{}/", address)).unwrap();
    easy.proxy("").unwrap();
    easy.timeout(TIMEOUT).unwrap();
}

struct Probe {
    port: u16,
    calls: usize,
    debug_calls: usize,
    allow: bool,
    panic_once: bool,
}

impl Probe {
    fn new(port: u16) -> Self {
        Self {
            port,
            calls: 0,
            debug_calls: 0,
            allow: true,
            panic_once: false,
        }
    }
}

impl Handler for Probe {
    fn prereq(&mut self, remote: &str, local: &str, remote_port: u16, local_port: u16) -> bool {
        assert_eq!(remote, "127.0.0.1");
        assert_eq!(local, "127.0.0.1");
        assert_eq!(remote_port, self.port);
        assert_ne!(local_port, 0);
        self.calls += 1;
        if self.panic_once {
            self.panic_once = false;
            panic!("prereq panic");
        }
        self.allow
    }

    fn debug(&mut self, _: InfoType, _: &[u8]) {
        self.debug_calls += 1;
    }
}

#[test]
fn handler_prereq_is_opt_in_across_moves_reuse_and_reset() {
    if !supported() {
        return;
    }
    let (address, server) = keepalive_server(vec![EMPTY; 6]);
    let mut easy = Easy2::new(Probe::new(address.port()));
    configure(&mut easy, address);
    easy.perform().unwrap();
    assert_eq!(easy.get_ref().calls, 0);

    easy.prereq(true).unwrap();
    easy.perform().unwrap();
    assert_eq!(easy.get_ref().calls, 1);
    easy = thread::spawn(move || {
        easy.perform().unwrap();
        easy
    })
    .join()
    .unwrap();
    assert_eq!(easy.get_ref().calls, 2);

    easy.reset();
    configure(&mut easy, address);
    easy.perform().unwrap();
    assert_eq!(easy.get_ref().calls, 2, "reset disables the callback");
    easy.prereq(true).unwrap();
    easy.perform().unwrap();
    assert_eq!(easy.get_ref().calls, 3);
    easy.prereq(false).unwrap();
    easy.perform().unwrap();
    assert_eq!(easy.get_ref().calls, 3);
    assert_eq!(easy.get_ref().debug_calls, 0);
    assert_eq!(easy.num_connects().unwrap(), 0, "reuse the same socket");
    assert_eq!(server.join().unwrap().len(), 6);
}

#[test]
fn handler_prereq_runs_for_each_followed_redirect() {
    if !supported() {
        return;
    }
    let (address, server) = keepalive_server(vec![
        b"HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\n\r\n",
        EMPTY,
    ]);
    let mut easy = Easy2::new(Probe::new(address.port()));
    configure(&mut easy, address);
    easy.prereq(true).unwrap();
    easy.follow_location(true).unwrap();
    easy.perform().unwrap();
    assert_eq!(easy.get_ref().calls, 2);
    let requests = server.join().unwrap();
    assert!(requests[0].starts_with(b"GET / HTTP/1.1\r\n"));
    assert!(requests[1].starts_with(b"GET /final HTTP/1.1\r\n"));
}

#[test]
fn handler_prereq_can_abort_before_sending_headers() {
    if !supported() {
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || read_head(&mut listener.accept().unwrap().0));
    let mut probe = Probe::new(address.port());
    probe.allow = false;
    let mut easy = Easy2::new(probe);
    configure(&mut easy, address);
    easy.prereq(true).unwrap();
    assert!(easy.perform().unwrap_err().is_aborted_by_callback());
    assert_eq!(easy.get_ref().calls, 1);
    drop(easy);
    assert!(server.join().unwrap().is_empty());
}

#[test]
fn handler_prereq_panic_is_rethrown_and_does_not_poison_reuse() {
    if !supported() {
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        assert!(read_head(&mut listener.accept().unwrap().0).is_empty());
        let (mut stream, _) = listener.accept().unwrap();
        assert!(read_head(&mut stream).starts_with(b"GET / "));
        stream.write_all(EMPTY).unwrap();
    });
    let mut probe = Probe::new(address.port());
    probe.panic_once = true;
    let mut easy = Easy2::new(probe);
    configure(&mut easy, address);
    easy.prereq(true).unwrap();
    let panic = catch_unwind(AssertUnwindSafe(|| easy.perform())).unwrap_err();
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"prereq panic"));
    easy.perform().unwrap();
    assert_eq!(easy.get_ref().calls, 2);
    server.join().unwrap();
}

#[test]
fn handler_prereq_panic_is_rethrown_from_multi() {
    if !supported() {
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || read_head(&mut listener.accept().unwrap().0));
    let mut probe = Probe::new(address.port());
    probe.panic_once = true;
    let mut easy = Easy2::new(probe);
    configure(&mut easy, address);
    easy.prereq(true).unwrap();
    let multi = Multi::new();
    let handle = multi.add2(easy).unwrap();
    let panic = catch_unwind(AssertUnwindSafe(|| {
        while multi.perform().unwrap() != 0 {
            multi.wait(&mut [], Duration::from_millis(100)).unwrap();
        }
    }))
    .unwrap_err();
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"prereq panic"));
    drop(handle);
    drop(multi);
    assert!(server.join().unwrap().is_empty());
}

#[test]
fn easy_prereq_supports_owned_and_scoped_callbacks() {
    if !supported() {
        return;
    }
    let (address, server) = keepalive_server(vec![EMPTY; 5]);
    let calls = Arc::new(AtomicUsize::new(0));
    let owned_calls = Arc::clone(&calls);
    let mut easy = Easy::new();
    easy.url(&format!("http://{}/", address)).unwrap();
    easy.proxy("").unwrap();
    easy.timeout(TIMEOUT).unwrap();
    easy.prereq_function(move |remote, local, remote_port, local_port| {
        assert_eq!(remote, "127.0.0.1");
        assert_eq!(local, "127.0.0.1");
        assert_eq!(remote_port, address.port());
        assert_ne!(local_port, 0);
        owned_calls.fetch_add(1, Ordering::SeqCst);
        true
    })
    .unwrap();
    easy.perform().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let mut scoped_calls = 0;
    {
        let mut transfer = easy.transfer();
        transfer
            .prereq_function(|_, _, port, _| {
                assert_eq!(port, address.port());
                scoped_calls += 1;
                true
            })
            .unwrap();
        transfer.perform().unwrap();
    }
    assert_eq!(scoped_calls, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    easy.perform().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    easy.reset();
    easy.url(&format!("http://{}/", address)).unwrap();
    easy.proxy("").unwrap();
    easy.timeout(TIMEOUT).unwrap();
    easy.perform().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        Arc::strong_count(&calls),
        2,
        "reset retains the closure while disabling its callback"
    );
    easy.prereq(true).unwrap();
    easy.perform().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    drop(easy);
    assert_eq!(Arc::strong_count(&calls), 1);
    assert_eq!(server.join().unwrap().len(), 5);
}

#[test]
fn prereq_reports_unsupported_libcurl_instead_of_panicking() {
    struct Empty;
    impl Handler for Empty {}
    let mut easy = Easy2::new(Empty);
    let result = easy.prereq(true);
    if supported() {
        result.unwrap();
    } else {
        assert!(result.unwrap_err().is_unknown_option());
    }
}
