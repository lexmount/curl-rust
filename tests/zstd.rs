#![cfg(feature = "zstd")]

use curl::easy::Easy;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

#[test]
fn native_zstd_decoding_negotiates_and_accounts_for_framed_empty_and_invalid_bodies() {
    const DATA: &[u8] = &[40, 181, 47, 253, 32, 3, 25, 0, 0, 97, 98, 99];
    const EMPTY: &[u8] = &[40, 181, 47, 253, 32, 0, 1, 0, 0];
    assert!(curl::Version::get().feature_zstd());

    for (encoded, decoded, invalid) in [
        (DATA, b"abc".as_slice(), false),
        (EMPTY, b"".as_slice(), false),
        (b"bad".as_slice(), b"".as_slice(), true),
    ] {
        for chunked in [false, true] {
            for method in ["GET", "HEAD"] {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let address = listener.local_addr().unwrap();
                let server = thread::spawn(move || {
                    let (mut stream, _) = listener.accept().unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(10)))
                        .unwrap();
                    let mut request = Vec::new();
                    let mut byte = [0];
                    while !request.ends_with(b"\r\n\r\n") {
                        stream.read_exact(&mut byte).unwrap();
                        request.push(byte[0]);
                    }
                    let request = String::from_utf8(request).unwrap();
                    assert!(request.starts_with(method));
                    assert!(request.lines().any(|line| {
                        line.to_ascii_lowercase().starts_with("accept-encoding:")
                            && line
                                .split(':')
                                .nth(1)
                                .unwrap()
                                .split(',')
                                .any(|coding| coding.trim() == "zstd")
                    }));
                    let framing = if chunked {
                        "Transfer-Encoding: chunked".to_owned()
                    } else {
                        format!("Content-Length: {}", encoded.len())
                    };
                    write!(stream, "HTTP/1.1 200 OK\r\nContent-Encoding: zstd\r\n{framing}\r\nConnection: close\r\n\r\n").unwrap();
                    if method == "GET" {
                        if chunked {
                            write!(stream, "{:x};test=framing\r\n", encoded.len()).unwrap();
                            stream.write_all(encoded).unwrap();
                            stream
                                .write_all(b"\r\n0\r\nX-Trailer: present\r\n\r\n")
                                .unwrap();
                        } else {
                            stream.write_all(encoded).unwrap();
                        }
                    }
                });
                let mut easy = Easy::new();
                easy.url(&format!("http://{address}/zstd")).unwrap();
                easy.proxy("").unwrap();
                easy.timeout(Duration::from_secs(10)).unwrap();
                easy.accept_encoding("").unwrap();
                easy.nobody(method == "HEAD").unwrap();
                let mut actual = Vec::new();
                let result = {
                    let mut transfer = easy.transfer();
                    transfer
                        .write_function(|bytes| {
                            actual.extend_from_slice(bytes);
                            Ok(bytes.len())
                        })
                        .unwrap();
                    transfer.perform()
                };
                assert_eq!(result.is_err(), method == "GET" && invalid);
                assert_eq!(
                    actual,
                    if method == "HEAD" {
                        b"".as_slice()
                    } else {
                        decoded
                    }
                );
                assert_eq!(
                    easy.download_size().unwrap(),
                    if method == "HEAD" {
                        0.0
                    } else {
                        encoded.len() as f64
                    }
                );
                server.join().unwrap();
            }
        }
    }
}
