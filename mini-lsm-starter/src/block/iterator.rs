use std::sync::Arc;

use bytes::{Buf, Bytes};

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl Block {
    fn get_first_key(&self) -> KeyVec {
        let mut buf = &self.data[..];
        let key_len = buf.get_u16();
        let key = &buf[..key_len as usize];
        KeyVec::from_vec(key.to_vec())
    }
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            first_key: KeyVec::new(),
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0);
    }

    fn seek_to_idx(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        let offset = self.block.offsets[idx] as usize;
        let mut entry = &self.block.data[offset..];
        let key_len = entry.get_u16() as usize;
        self.key.clear();
        self.key.append(&entry[..key_len]);
        entry.advance(key_len);
        let value_len = entry.get_u16() as usize;
        let value_offset_start = offset + SIZEOF_U16 + key_len + SIZEOF_U16;
        self.value_range = (value_offset_start, value_offset_start + value_len);
        entry.advance(value_len);
        self.idx = idx;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_idx(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        for offset in &self.block.offsets {
            let start_offset = *offset as usize;
            let mut entry = &self.block.data[start_offset..];
            let key_len = entry.get_u16() as usize;
            let k = &entry[..key_len];
            entry.advance(key_len);
            let value_offset_start = start_offset + SIZEOF_U16 + key_len + SIZEOF_U16;
            match k.cmp(key.raw_ref()) {
                std::cmp::Ordering::Less => {
                    let value_len = entry.get_u16() as usize;
                    entry.advance(value_len);
                    self.idx += 1;
                    continue;
                }
                std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => {
                    self.key.clear();
                    self.key.append(k);
                    let value_len = entry.get_u16() as usize;
                    self.value_range = (value_offset_start, value_offset_start + value_len);
                    return;
                }
            }
        }
    }
}
