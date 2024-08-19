#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use std::{io::Read, mem};

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

const SIZEOF_U16: usize = mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(self.offsets.len() as u16);
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let data_len = data.len();
        let mut last_two_bytes = &data[data_len - SIZEOF_U16..];
        let num_of_elements = last_two_bytes.get_u16() as usize;
        let offset_start_idx = data_len - SIZEOF_U16 - (num_of_elements * SIZEOF_U16);
        let offsets_bytes = &data[offset_start_idx..data_len - SIZEOF_U16];

        let offsets: Vec<u16> = offsets_bytes
            .chunks_exact(SIZEOF_U16)
            .map(|mut bytes| bytes.get_u16())
            .collect();

        let data = data[..offset_start_idx].to_vec();

        Self { data, offsets }
    }
}
