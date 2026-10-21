#![cfg(feature = "browser-response-headers")]

use curl::easy::Easy;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

fn fetch(chunks: Vec<Vec<u8>>) -> (Result<(), curl::Error>, Vec<Vec<u8>>, Vec<u8>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut request = Vec::new();
        let mut buf = [0; 1024];
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let count = socket.read(&mut buf).unwrap();
            assert_ne!(count, 0);
            request.extend_from_slice(&buf[..count]);
        }
        for chunk in chunks {
            if socket.write_all(&chunk).is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
    });
    let mut easy = Easy::new();
    easy.url(&format!("http://{address}/")).unwrap();
    easy.proxy("").unwrap();
    easy.timeout(Duration::from_secs(5)).unwrap();
    let mut headers = Vec::new();
    let mut body = Vec::new();
    let result = {
        let mut transfer = easy.transfer();
        transfer
            .header_function(|line| {
                headers.push(line.to_vec());
                true
            })
            .unwrap();
        transfer
            .write_function(|bytes| {
                body.extend_from_slice(bytes);
                Ok(bytes.len())
            })
            .unwrap();
        transfer.perform()
    };
    server.join().unwrap();
    (result, headers, body)
}

#[test]
fn bare_cr_response_fields_match_browser_parsing() {
    for (fields, expected) in [
        ("X-Test: one\rtwo\r\n", vec!["X-Test: one\r\n"]),
        (
            "X-Test: one\rX-Other: two\r\n",
            vec!["X-Test: one\r\n", "X-Other: two\r\n"],
        ),
        ("X-Test: one\r \ttwo\r\n", vec!["X-Test: one two\r\n"]),
        (
            "X-Test: one\r\rX-Other: two\r\n",
            vec!["X-Test: one\r\n", "X-Other: two\r\n"],
        ),
        ("\rX-Test: one\r\n", vec!["X-Test: one\r\n"]),
    ] {
        let response = format!(
            "HTTP/1.1 200 OK\r\n{fields}Content-Length: 3\r\nConnection: close\r\n\r\nabcEXTRA"
        );
        // Send both whole lines and CR split from the following segment/LF.
        for chunks in [
            vec![response.as_bytes().to_vec()],
            response
                .as_bytes()
                .split_inclusive(|byte| *byte == b'\r')
                .map(<[u8]>::to_vec)
                .collect(),
        ] {
            let (result, headers, body) = fetch(chunks);
            result.unwrap_or_else(|error| panic!("{:?}: {}", fields, error));
            for line in &expected {
                assert!(headers.contains(&line.as_bytes().to_vec()), "{:?}", headers);
            }
            assert_eq!(body, b"abc", "{fields:?}");
        }
    }
}

#[test]
fn bare_cr_framing_fields_are_interpreted_before_body_delivery() {
    for (fields, body) in [
        ("X-Test: one\rContent-Length: 3\r\n", "abcEXTRA"),
        (
            "X-Test: one\rTransfer-Encoding: chunked\r\n",
            "3\r\nabc\r\n0\r\n\r\n",
        ),
    ] {
        let response = format!("HTTP/1.1 200 OK\r\n{fields}Connection: close\r\n\r\n{body}");
        let (result, _, body) = fetch(vec![response.into_bytes()]);
        result.unwrap();
        assert_eq!(body, b"abc");
    }
}

#[test]
fn bare_cr_does_not_bypass_nul_or_content_length_validation() {
    for fields in [
        "X-Test: one\rignored\0segment\r\n",
        "Content-Length: 3\rContent-Length: 4\r\n",
    ] {
        let response = format!("HTTP/1.1 200 OK\r\n{fields}Connection: close\r\n\r\nabc");
        let (result, _, _) = fetch(vec![response.into_bytes()]);
        assert!(result.is_err(), "{:?}", fields);
    }
}

#[test]
fn ignored_cr_segments_still_count_towards_the_header_limit() {
    let mut response = b"HTTP/1.1 200 OK\r\n".to_vec();
    for _ in 0..10 {
        response.extend_from_slice(b"X-Test: one\r");
        response.extend(std::iter::repeat_n(b'a', 40_000));
        response.extend_from_slice(b"\r\n");
    }
    response.extend_from_slice(b"Content-Length: 3\r\nConnection: close\r\n\r\nabc");
    let (result, _, _) = fetch(vec![response]);
    assert!(result.is_err());
}
