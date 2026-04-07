/// block_interface.rs — Buffer Manager / Disk I/O Layer
///
/// This module is the ONLY place that talks to the disk simulator.
/// Everything else (executor, output) calls methods here to read/write blocks.
///
/// Role: translate high-level requests ("give me block 42") into the text
/// commands that the disk simulator understands over FD 3/FD 4.
///
/// Disk command protocol (text lines over FD 4, responses over FD 3):
///   "get block-size\n"              → "<bytes>\n"
///   "get file start-block <id>\n"   → "<block_id>\n"
///   "get file num-blocks <id>\n"    → "<count>\n"
///   "get anon-start-block\n"        → "<block_id>\n"
///   "get block <start> <count>\n"   → <count * block_size raw bytes> (no \n)
///   "put block <start> <count>\n"   followed by <count * block_size raw bytes>
///
/// Anonymous (scratch) region: writable blocks at ids >= anon_start.
/// We allocate scratch blocks by bumping a counter (next_anon).
use std::io::{BufRead, Write};

use anyhow::Result;

/// Wraps disk I/O and exposes simple read/write/allocate operations.
///
/// R: a buffered reader over FD 3 (disk → database)
/// W: a writer over FD 4 (database → disk)
pub struct BlockInterface<R: BufRead, W: Write> {
    /// Buffered reader from the disk simulator (FD 3)
    reader: R,
    /// Writer to the disk simulator (FD 4)
    writer: W,
    /// Size of one block in bytes (e.g. 4096)
    pub block_size: usize,
    /// The first block ID in the anonymous (scratch) region
    pub anon_start: u64,
    /// The next free block ID for scratch allocation
    next_anon: u64,
}

impl<R: BufRead, W: Write> BlockInterface<R, W> {
    /// Create a new BlockInterface.
    ///
    /// Sends "get anon-start-block" to the disk to find where scratch space begins.
    /// block_size and memory_limit_mb come from earlier queries in main.rs.
    pub fn new(
        mut reader: R,
        mut writer: W,
        block_size: u64,
        _memory_limit_mb: u64,
    ) -> Result<Self> {
        // Ask the disk where the writable (anonymous) region begins.
        // We must write to this region (not the read-only file region).
        writer.write_all(b"get anon-start-block\n")?;
        writer.flush()?;

        let mut line = String::new();
        reader.read_line(&mut line)?;
        let anon_start: u64 = line.trim().parse()?;

        Ok(Self {
            reader,
            writer,
            block_size: block_size as usize,
            anon_start,
            next_anon: anon_start,
        })
    }

    // ─── Reading from disk ─────────────────────────────────────────────────────

    /// Read `num_blocks` consecutive blocks starting at `start_block_id`.
    /// The raw bytes are written into `out`, which is resized accordingly.
    ///
    /// Used by ScanIter to fetch one block at a time from a table file.
    pub fn read_blocks(
        &mut self,
        start_block_id: u64,
        num_blocks: u64,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        let total_bytes = num_blocks as usize * self.block_size;
        out.resize(total_bytes, 0);

        // Send the read command
        let cmd = format!("get block {} {}\n", start_block_id, num_blocks);
        self.writer.write_all(cmd.as_bytes())?;
        self.writer.flush()?;

        // Read exactly the expected number of raw bytes (no newline at end)
        self.reader.read_exact(out)?;
        Ok(())
    }

    /// Ask the disk for the starting block ID of a named file.
    /// (Used by ScanIter to locate a table file on first use.)
    pub fn get_file_start(&mut self, file_id: &str) -> Result<u64> {
        let cmd = format!("get file start-block {}\n", file_id);
        self.writer.write_all(cmd.as_bytes())?;
        self.writer.flush()?;

        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        Ok(line.trim().parse()?)
    }

    /// Ask the disk for the number of blocks a named file spans.
    /// (Used by ScanIter to know when to stop reading.)
    pub fn get_file_num_blocks(&mut self, file_id: &str) -> Result<u64> {
        let cmd = format!("get file num-blocks {}\n", file_id);
        self.writer.write_all(cmd.as_bytes())?;
        self.writer.flush()?;

        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        Ok(line.trim().parse()?)
    }

    // ─── Scratch (anonymous) region ────────────────────────────────────────────

    /// Allocate `n` contiguous scratch blocks. Returns the starting block ID.
    ///
    /// No disk I/O happens here — the block IDs are reserved by bumping a counter.
    /// You MUST call write_scratch before reading the allocated blocks.
    pub fn alloc_scratch(&mut self, n: u64) -> u64 {
        let start = self.next_anon;
        self.next_anon += n;
        start
    }

    /// Write `data` to scratch blocks starting at `block_id`.
    ///
    /// `data.len()` must be a multiple of block_size.
    /// `block_id` must be >= anon_start.
    ///
    /// Used by external sort (SortIter) to spill sorted runs to disk.
    pub fn write_scratch(&mut self, block_id: u64, data: &[u8]) -> Result<()> {
        debug_assert!(
            data.len() % self.block_size == 0,
            "write_scratch: data length must be a multiple of block_size"
        );
        let num_blocks = data.len() / self.block_size;

        let cmd = format!("put block {} {}\n", block_id, num_blocks);
        self.writer.write_all(cmd.as_bytes())?;
        // Raw bytes follow immediately (no extra newline)
        self.writer.write_all(data)?;
        self.writer.flush()?;
        Ok(())
    }
}
