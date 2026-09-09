//! WebSocket frame metadata and options.

use std::ops::{BitOr, BitOrAssign};

use libc::c_long;

/// Flags describing a WebSocket frame or a call to `ws_send`.
///
/// Combine flags with `|`. Libcurl reports the text or binary type on every
/// fragment, with `CONT` set on all fragments except the last one. These flags
/// describe frames, not complete messages; callers assemble messages themselves.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WsFlags(u32);

impl WsFlags {
    /// No flags, used when sending in raw mode.
    pub const NONE: Self = Self(0);
    /// Text payload. The application is responsible for UTF-8 validation.
    pub const TEXT: Self = Self(curl_sys::CURLWS_TEXT as u32);
    /// Binary payload.
    pub const BINARY: Self = Self(curl_sys::CURLWS_BINARY as u32);
    /// More frames follow in this fragmented message.
    pub const CONT: Self = Self(curl_sys::CURLWS_CONT as u32);
    /// Close control frame.
    pub const CLOSE: Self = Self(curl_sys::CURLWS_CLOSE as u32);
    /// Ping control frame.
    pub const PING: Self = Self(curl_sys::CURLWS_PING as u32);
    /// Supply a frame's payload across multiple calls to `ws_send`.
    pub const OFFSET: Self = Self(curl_sys::CURLWS_OFFSET as u32);
    /// Pong control frame.
    pub const PONG: Self = Self(curl_sys::CURLWS_PONG as u32);

    /// Constructs flags, retaining unknown bits for forward compatibility.
    pub const fn from_bits(bits: u32) -> Self {
        Self(bits)
    }

    /// Returns the underlying libcurl bitmask.
    pub const fn bits(self) -> u32 {
        self.0
    }

    /// Returns whether all of the given flags are set.
    pub const fn contains(self, flags: Self) -> bool {
        self.0 & flags.0 == flags.0
    }
}

impl BitOr for WsFlags {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl BitOrAssign for WsFlags {
    fn bitor_assign(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

/// An owned snapshot of libcurl's WebSocket frame metadata.
///
/// Unlike the native `curl_ws_frame` pointer, this value remains valid after
/// another WebSocket operation or after the easy handle is dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WsFrame {
    flags: WsFlags,
    offset: u64,
    bytes_left: u64,
    len: usize,
}

impl WsFrame {
    pub(crate) fn from_raw(frame: &curl_sys::curl_ws_frame) -> Self {
        Self {
            flags: WsFlags::from_bits(frame.flags as u32),
            offset: frame.offset as u64,
            bytes_left: frame.bytesleft as u64,
            len: frame.len,
        }
    }

    /// The frame type and fragmentation flags.
    pub fn flags(&self) -> WsFlags {
        self.flags
    }

    /// Offset of this chunk within the frame payload.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Payload bytes remaining in this frame after this chunk.
    ///
    /// Zero marks the end of a frame, which is only the end of the message
    /// when `WsFlags::CONT` is also absent.
    pub fn bytes_left(&self) -> u64 {
        self.bytes_left
    }

    /// Size of the current payload chunk, without WebSocket headers.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the current payload chunk is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// WebSocket behavior options for `ws_options`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WsOptions {
    pub(crate) bits: c_long,
}

impl WsOptions {
    /// Creates options with libcurl's default behavior.
    pub fn new() -> Self {
        Self::default()
    }

    /// Pass raw WebSocket bytes through the read and write callbacks.
    ///
    /// In raw mode the application implements frame encoding, decoding, and
    /// control handling. Use `WsFlags::NONE` and a frame size of zero with
    /// `ws_send` in this mode.
    pub fn raw_mode(&mut self, enabled: bool) -> &mut Self {
        self.flag(curl_sys::CURLWS_RAW_MODE, enabled)
    }

    /// Disable libcurl's automatic replies to ping frames.
    ///
    /// The application must send the corresponding pong itself. This option
    /// requires libcurl 8.14.0 or newer.
    pub fn no_auto_pong(&mut self, enabled: bool) -> &mut Self {
        self.flag(curl_sys::CURLWS_NOAUTOPONG, enabled)
    }

    fn flag(&mut self, flag: c_long, enabled: bool) -> &mut Self {
        if enabled {
            self.bits |= flag;
        } else {
            self.bits &= !flag;
        }
        self
    }
}
