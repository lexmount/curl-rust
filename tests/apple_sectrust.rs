#[cfg(all(target_vendor = "apple", feature = "apple-sectrust"))]
#[test]
fn bundled_build_enables_apple_sectrust() {
    use std::ffi::CStr;

    assert!(curl::Version::get().vendored());

    unsafe {
        let info = curl_sys::curl_version_info(curl_sys::CURLVERSION_NOW);
        assert!(!info.is_null());

        let mut feature = (*info).feature_names;
        assert!(!feature.is_null());

        while !(*feature).is_null() {
            if CStr::from_ptr(*feature).to_bytes() == b"AppleSecTrust" {
                return;
            }
            feature = feature.add(1);
        }
    }

    panic!("bundled libcurl was not built with Apple SecTrust");
}

#[cfg(all(target_vendor = "apple", feature = "apple-sectrust"))]
#[test]
fn explicit_certificate_file_is_honored_after_new_and_reset() {
    use std::net::TcpListener;
    use std::process::Command;
    use std::time::Duration;

    const CHILD: &str = "CURL_RUST_TEST_INVALID_CA_OVERRIDE";
    if std::env::var_os(CHILD).is_none() {
        // Give the child its own environment; other tests may create handles
        // concurrently and must keep their normal trust configuration.
        let missing =
            std::env::temp_dir().join(format!("curl-rust-missing-ca-{}.pem", std::process::id()));
        assert!(!missing.exists());
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "explicit_certificate_file_is_honored_after_new_and_reset",
            ])
            .env(CHILD, "1")
            .env("SSL_CERT_FILE", missing)
            .env_remove("SSL_CERT_DIR")
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        return;
    }

    // Connecting works, but configuring TLS must reject the explicit missing
    // CA file before it waits for a server handshake or falls back to SecTrust.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("https://{}/", listener.local_addr().unwrap());
    let mut easy = curl::easy::Easy::new();
    for _ in 0..2 {
        easy.url(&url).unwrap();
        easy.proxy("").unwrap();
        easy.timeout(Duration::from_secs(2)).unwrap();
        let error = easy.perform().unwrap_err();
        assert!(error.is_ssl_cacert_badfile(), "{error}");
        easy.reset();
    }
}
