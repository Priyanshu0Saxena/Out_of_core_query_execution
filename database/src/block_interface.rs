// All disk communication helpers
// Talk to disk simulator - get block size, get file info, read blocks, write scratch blocks
use anyhow::Result;

pub struct BlockInterface {
    // TODO: fields like block_size, anon_start_block,
    // next_free_anon_block, disk_reader, disk_writer
}

impl BlockInterface {
    /// Called once at startup — fetches block_size and anon_start_block from disk
    pub fn new(...) -> Result<Self> {
        todo!()
    }

    /// Read num_blocks blocks starting at start_block_id from disk
    /// Returns raw bytes
    pub fn read_blocks(&mut self, start_block_id: u64, num_blocks: u64) -> Result<Vec<u8>> {
        todo!()
    }

    /// Write raw bytes to anonymous scratch region
    /// Returns the block_id where it was written
    pub fn write_scratch(&mut self, data: &[u8]) -> Result<u64> {
        todo!()
    }

    /// Get file start block id from disk
    pub fn get_file_start_block(&mut self, file_id: &str) -> Result<u64> {
        todo!()
    }

    /// Get number of blocks a file spans
    pub fn get_file_num_blocks(&mut self, file_id: &str) -> Result<u64> {
        todo!()
    }
}