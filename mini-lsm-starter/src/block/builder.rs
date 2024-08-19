use std::mem;

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

/// Builds a block.
#[derive(Debug)]
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size: block_size,
            first_key: KeyVec::new(),
        }
    }

    fn current_size(&self) -> usize {
        SIZEOF_U16 + self.offsets.len() * SIZEOF_U16 + self.data.len()
    }

    fn estimated_size(&self, key: KeySlice, value: &[u8]) -> usize {
        self.current_size() + key.len() + value.len() + 3 * SIZEOF_U16
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if !self.is_empty() && self.estimated_size(key, value) >= self.block_size {
            return false;
        }

        let key_len = key.len() as u16;
        let key_bytes = key.into_inner();
        let value_len = value.len() as u16;
        let value_bytes = value;

        self.data.extend_from_slice(&key_len.to_ne_bytes()[..]);
        self.data.extend_from_slice(key_bytes);
        self.data.extend_from_slice(&value_len.to_ne_bytes()[..]);
        self.data.extend_from_slice(value_bytes);

        self.offsets.push(self.data.len() as u16);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
