use std::env;

fn main() {
    println!(
        "cargo:rustc-check-cfg=cfg(\
            need_openssl_init,\
            need_openssl_probe,\
        )"
    );
    // OpenSSL >= 1.1.0 can be initialized concurrently and is initialized correctly by libcurl.
    // <= 1.0.2 need locking callbacks, which are provided by openssl_sys::init().
    let use_openssl = match env::var("DEP_OPENSSL_VERSION_NUMBER") {
        Ok(version) => {
            let version = u64::from_str_radix(&version, 16).unwrap();
            if version < 0x1_01_00_00_0 {
                println!("cargo:rustc-cfg=need_openssl_init");
            }
            true
        }
        // openssl-sys exposes AWS-LC through the OpenSSL-compatible API, but
        // reports the backend through metadata instead of a version number.
        Err(_) => env::var_os("DEP_OPENSSL_AWSLC").is_some(),
    };

    if use_openssl {
        // The system libcurl should have the default certificate paths configured.
        if env::var_os("DEP_CURL_STATIC").is_some() {
            println!("cargo:rustc-cfg=need_openssl_probe");
        }
    }
}
