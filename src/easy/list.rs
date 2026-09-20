use std::ffi::{CStr, CString};
use std::fmt;
use std::ptr;

use crate::Error;

/// A linked list of NUL-terminated byte strings.
pub struct List {
    raw: *mut curl_sys::curl_slist,
}

/// An iterator over `List`
#[derive(Clone)]
pub struct Iter<'a> {
    _me: &'a List,
    cur: *mut curl_sys::curl_slist,
}

pub fn raw(list: &List) -> *mut curl_sys::curl_slist {
    list.raw
}

pub unsafe fn from_raw(raw: *mut curl_sys::curl_slist) -> List {
    List { raw }
}

unsafe impl Send for List {}

impl List {
    /// Creates a new empty list of strings.
    pub fn new() -> List {
        List {
            raw: ptr::null_mut(),
        }
    }

    /// Appends some data into this list.
    pub fn append(&mut self, data: &str) -> Result<(), Error> {
        self.append_bytes(data.as_bytes())
    }

    /// Appends bytes into this list without requiring UTF-8.
    ///
    /// The list owns a copy of the data. Interior NUL bytes are not allowed.
    /// If allocation fails or the data contains a NUL byte, this returns an
    /// error and leaves the existing list unchanged.
    pub fn append_bytes(&mut self, data: &[u8]) -> Result<(), Error> {
        let data = CString::new(data)?;
        unsafe {
            let raw = curl_sys::curl_slist_append(self.raw, data.as_ptr());
            if raw.is_null() {
                return Err(Error::new(curl_sys::CURLE_OUT_OF_MEMORY));
            }
            self.raw = raw;
            Ok(())
        }
    }

    /// Returns an iterator over the nodes in this list.
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            _me: self,
            cur: self.raw,
        }
    }
}

impl fmt::Debug for List {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_list()
            .entries(self.iter().map(String::from_utf8_lossy))
            .finish()
    }
}

impl<'a> IntoIterator for &'a List {
    type IntoIter = Iter<'a>;
    type Item = &'a [u8];

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

impl Drop for List {
    fn drop(&mut self) {
        unsafe { curl_sys::curl_slist_free_all(self.raw) }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        if self.cur.is_null() {
            return None;
        }

        unsafe {
            let ret = Some(CStr::from_ptr((*self.cur).data).to_bytes());
            self.cur = (*self.cur).next;
            ret
        }
    }
}

impl<'a> fmt::Debug for Iter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_list()
            .entries(self.clone().map(String::from_utf8_lossy))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::List;

    #[test]
    fn append_bytes_owns_non_utf8_data_and_preserves_order() {
        let mut list = List::new();
        let mut bytes = b"X-Raw: \xe9\xff".to_vec();
        list.append_bytes(&bytes).unwrap();
        bytes.fill(b'x');
        drop(bytes);
        list.append("X-Raw: é").unwrap();
        list.append_bytes(b"").unwrap();

        assert_eq!(
            list.iter().collect::<Vec<_>>(),
            vec![b"X-Raw: \xe9\xff".as_slice(), "X-Raw: é".as_bytes(), b""],
        );
    }

    #[test]
    fn append_rejects_nul_without_changing_existing_entries() {
        let mut list = List::new();
        list.append_bytes(b"X-Raw: \xff").unwrap();
        assert!(list.append_bytes(b"before\0after").is_err());
        assert!(list.append("before\0after").is_err());
        list.append_bytes(b"X-Raw: after").unwrap();

        assert_eq!(
            list.iter().collect::<Vec<_>>(),
            vec![b"X-Raw: \xff".as_slice(), b"X-Raw: after"],
        );
    }
}
