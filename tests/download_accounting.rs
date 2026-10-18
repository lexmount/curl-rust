#![cfg(feature = "static-curl")]

use curl::easy::{Easy2, Handler, WriteError};
use curl::multi::Multi;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Default)]
struct Sink {
    decoded: usize,
    progress: u64,
    reject_write: bool,
    pause_once: bool,
    paused: bool,
}

impl Handler for Sink {
    fn write(&mut self, bytes: &[u8]) -> Result<usize, WriteError> {
        if self.reject_write {
            return Ok(0);
        }
        if self.pause_once {
            self.pause_once = false;
            self.paused = true;
            return Err(WriteError::Pause);
        }
        self.paused = false;
        self.decoded += bytes.len();
        Ok(bytes.len())
    }

    fn progress(&mut self, _: f64, now: f64, _: f64, _: f64) -> bool {
        self.progress = self.progress.max(now as u64);
        true
    }
}

fn serve(coding: &str, chunked: bool, head: bool, body: &[u8]) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let coding = coding.to_owned();
    let body = body.to_vec();
    let server = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut request = Vec::new();
        let mut byte = [0];
        while !request.ends_with(b"\r\n\r\n") {
            socket.read_exact(&mut byte).unwrap();
            request.push(byte[0]);
        }
        let mut response = Vec::new();
        let framing = if chunked {
            "Transfer-Encoding: chunked".to_owned()
        } else {
            format!("Content-Length: {}", body.len())
        };
        write!(response, "HTTP/1.1 200 OK\r\nContent-Encoding: {coding}\r\n{framing}\r\nConnection: close\r\n\r\n").unwrap();
        if !head {
            if chunked {
                if !body.is_empty() {
                    write!(response, "{:x};test=value\r\n", body.len()).unwrap();
                    response.extend_from_slice(&body);
                    response.extend_from_slice(b"\r\n");
                }
                response.extend_from_slice(b"0\r\nX-Trailer: present\r\n\r\n");
            } else {
                response.extend_from_slice(&body);
            }
        }
        socket.write_all(&response).unwrap();
    });
    (url, server)
}

fn transfer(url: &str, head: bool, reject_write: bool) -> Easy2<Sink> {
    let mut easy = Easy2::new(Sink {
        reject_write,
        ..Sink::default()
    });
    easy.url(url).unwrap();
    easy.noproxy("*").unwrap();
    easy.accept_encoding("").unwrap();
    easy.progress(true).unwrap();
    easy.nobody(head).unwrap();
    easy.timeout(Duration::from_secs(5)).unwrap();
    easy
}

#[test]
fn download_size_is_retained_when_content_decoding_fails() {
    for coding in ["gzip", "unrecognizedname", "gzip, deflate,Apple"] {
        for chunked in [false, true] {
            let (url, server) = serve(coding, chunked, false, b"abc");
            let easy = transfer(&url, false, false);
            let error = easy.perform().unwrap_err();
            assert_eq!(error.code(), 61, "{coding}/chunked={chunked}");
            assert_eq!(easy.download_size().unwrap(), 3.0);
            assert_eq!(easy.get_ref().progress, 3);
            assert_eq!(easy.get_ref().decoded, 0);
            server.join().unwrap();
        }
    }
}

#[test]
fn download_size_excludes_chunk_frames_and_absent_head_bodies() {
    const GZIP_EMPTY: &[u8] = &[
        31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    for (coding, chunked, head, body, encoded) in [
        ("unrecognizedname", true, false, b"".as_slice(), 0),
        ("gzip", false, true, b"abc".as_slice(), 0),
        ("gzip", false, false, GZIP_EMPTY, 20),
        ("gzip", true, false, GZIP_EMPTY, 20),
    ] {
        let (url, server) = serve(coding, chunked, head, body);
        let easy = transfer(&url, head, false);
        easy.perform().unwrap();
        assert_eq!(easy.download_size().unwrap(), encoded as f64);
        assert_eq!(easy.get_ref().progress, encoded);
        assert_eq!(easy.get_ref().decoded, 0);
        server.join().unwrap();
    }
}

#[test]
fn download_size_is_not_erased_by_a_rejected_application_write() {
    for chunked in [false, true] {
        let (url, server) = serve("identity", chunked, false, b"abc");
        let easy = transfer(&url, false, true);
        let error = easy.perform().unwrap_err();
        assert_eq!(error.code(), 23);
        assert_eq!(easy.download_size().unwrap(), 3.0);
        assert_eq!(easy.get_ref().progress, 3);
        assert_eq!(easy.get_ref().decoded, 0);
        server.join().unwrap();
    }
}

#[test]
fn resuming_a_paused_writer_does_not_count_payload_twice() {
    let (url, server) = serve("identity", true, false, b"abc");
    let mut easy = transfer(&url, false, false);
    easy.get_mut().pause_once = true;
    let multi = Multi::new();
    let handle = multi.add2(easy).unwrap();
    while !handle.get_ref().paused {
        assert_eq!(multi.perform().unwrap(), 1);
        multi.wait(&mut [], Duration::from_millis(10)).unwrap();
    }
    assert_eq!(handle.get_ref().decoded, 0);
    handle.unpause_write().unwrap();
    while multi.perform().unwrap() != 0 {
        multi.wait(&mut [], Duration::from_millis(10)).unwrap();
    }
    let mut completed = false;
    multi.messages(|message| {
        if let Some(result) = message.result_for2(&handle) {
            result.unwrap();
            completed = true;
        }
    });
    assert!(completed);
    let easy = multi.remove2(handle).unwrap();
    assert_eq!(easy.download_size().unwrap(), 3.0);
    assert_eq!(easy.get_ref().progress, 3);
    assert_eq!(easy.get_ref().decoded, 3);
    server.join().unwrap();
}
