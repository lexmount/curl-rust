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

#[test]
fn unterminated_bare_cr_headers_finish_at_plain_http_eof() {
    for prefix in ["", "HTTP/1.1 100 Continue\r\n\r\n"] {
        for byte in [b'X', b':', b'\t', b'\r', 0xff] {
            let mut response =
                format!("{prefix}HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r")
                    .into_bytes();
            response.extend_from_slice(&[byte, b'\n']);
            response.extend_from_slice(b"abc");
            let (result, headers, body) = fetch(vec![response]);
            result
                .unwrap_or_else(|error| panic!("prefix={:?}, byte={:#x}: {}", prefix, byte, error));
            assert!(headers.contains(&b"Content-Length: 3\r\n".to_vec()));
            assert_eq!(
                headers
                    .iter()
                    .filter(|line| line.as_slice() == b"\r\n")
                    .count(),
                usize::from(!prefix.is_empty()),
                "EOF does not manufacture a header/body separator"
            );
            assert!(body.is_empty(), "unterminated headers are not body bytes");
        }
    }
}

#[test]
fn plain_http_header_eof_flushes_pending_fields_without_decoding_a_body() {
    for fields in [
        "Content-Length: 3\r\nX-Last: yes",
        "Content-Length: 12\r\nContent-Encoding: gzip\r\n",
        "Transfer-Encoding: chunked\r\n",
    ] {
        let response = format!("HTTP/1.1 200 OK\r\nConnection: close\r\n{fields}");
        let (result, headers, body) = fetch(vec![response.into_bytes()]);
        result.unwrap_or_else(|error| panic!("{:?}: {}", fields, error));
        assert!(!headers.contains(&b"\r\n".to_vec()));
        assert!(body.is_empty());
        if fields.contains("X-Last") {
            assert!(headers.contains(&b"X-Last: yes\r\n".to_vec()));
        }
    }
}

#[test]
fn header_eof_keeps_nul_and_real_body_truncation_errors() {
    for response in [
        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nX-Test: one\0two",
        "HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nabc",
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nab",
        "HTTP/1.1 200 OK\r\nContent-Length: 3\r\nContent-Length: 4",
        "HTTP/1.1 100 Continue\r\n\r\n",
    ] {
        let (result, _, _) = fetch(vec![response.as_bytes().to_vec()]);
        assert!(result.is_err(), "{:?}", response);
    }
}

#[test]
fn leading_cr_status_slop_is_bounded_and_survives_informational_responses() {
    for prefix in ["", "HTTP/1.1 100 Continue\r\n\r\n"] {
        for count in 1..=5 {
            let response = format!(
                "{prefix}{}HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
                "\r".repeat(count)
            );
            let (result, _, body) = fetch(vec![response.into_bytes()]);
            if count <= 4 {
                result.unwrap_or_else(|error| {
                    panic!("prefix={:?}, count={}: {}", prefix, count, error)
                });
                assert_eq!(body, b"abc");
            } else {
                assert!(result.is_err(), "more than four bytes of status slop");
            }
        }
    }
}
