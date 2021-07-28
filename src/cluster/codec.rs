use std::ptr;

use bytes::{BufMut, Bytes, BytesMut};

pub const CLUSTER_ID_KEY: Bytes = Bytes::from_static(b"dcluster");
pub const CLUSTER_BOOTSTRAP_KEY: Bytes = Bytes::from_static(b"dcluster_bootstrap");

static REGION_KEY_PREFIX: &[u8] = b"dr";
static STORE_KEY_PREFIX: &[u8] = b"ds";
static RANGE_KEY_PREFIX: &[u8] = b"dt";
static RANGE_MAX_KEY: &[u8] = b"du";

fn put_order_byte(buf: &mut BytesMut, bytes: &[u8]) {
    let cap = ((bytes.len() - 1) / 8 + 1) * 9;
    buf.reserve(cap);
    let s = buf.chunk_mut();
    let mut dst = s.as_mut_ptr();
    let mut src = bytes.as_ptr();
    for _ in 0..cap / 9 - 1 {
        unsafe {
            ptr::copy_nonoverlapping(src, dst, 8);
            src = src.add(8);
            dst = dst.add(8);
            *dst = 0xff;
            dst = dst.add(1);
        }
    }
    let rest = bytes.len() % 8;
    unsafe {
        ptr::copy_nonoverlapping([0; 8].as_ptr(), dst, 8);
        ptr::copy_nonoverlapping(src, dst, rest);
        dst = dst.add(8);
        *dst = 0xff - rest as u8;
        buf.advance_mut(cap);
    }
}

pub fn region_key(id: u64) -> [u8; 10] {
    let mut key = [0; 10];
    key[..2].copy_from_slice(REGION_KEY_PREFIX);
    key[2..].copy_from_slice(&id.to_be_bytes());
    key
}

pub fn store_key(id: u64) -> [u8; 10] {
    let mut key = [0; 10];
    key[..2].copy_from_slice(STORE_KEY_PREFIX);
    key[2..].copy_from_slice(&id.to_be_bytes());
    key
}

pub fn region_range_key(key: &[u8], version: u64) -> Bytes {
    let mut buf = BytesMut::with_capacity(2 + key.len() + 8);
    if !key.is_empty() {
        buf.put_slice(RANGE_KEY_PREFIX);
        put_order_byte(&mut buf, key);
    } else {
        buf.put_slice(RANGE_MAX_KEY);
    }
    // make largest version appear first.
    buf.put_u64(!version);
    buf.freeze()
}

pub fn region_range_value(id: u64) -> Bytes {
    Bytes::copy_from_slice(&id.to_le_bytes())
}
