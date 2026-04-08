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
///
/// Buffer Pool (LRU):
///   Read-only file blocks (block_id < anon_start) are cached in an LRU buffer.
///   On cache hit we skip the disk round-trip.
///   When the cache is full the least-recently-used block is evicted.
///   Scratch blocks are NOT cached (they are written once, read rarely, and
///   caching them would waste memory).
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, Write};

use anyhow::Result;

/// Wraps disk I/O with an LRU block cache (buffer pool).
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

    // ── LRU Buffer Pool ────────────────────────────────────────────────────
    /// Cached block data: block_id → block bytes.
    cache: HashMap<u64, Vec<u8>>,
    /// LRU order: front = most-recently used, back = least-recently used.
    lru: VecDeque<u64>,
    /// Maximum number of blocks to keep in the cache.
    cache_capacity: usize,

    // ── Scratch block free list ────────────────────────────────────────────
    /// Freed scratch ranges: (start_block, num_blocks).
    /// When the sort merges two runs into one it frees the two source runs.
    /// alloc_scratch() consults this list before bumping next_anon.
    free_scratch: Vec<(u64, u64)>,
}

impl<R: BufRead, W: Write> BlockInterface<R, W> {
    /// Create a new BlockInterface with an LRU buffer pool.
    ///
    /// Cache capacity is derived from the memory limit:
    ///   capacity = (memory_limit_mb * 1MB) / block_size / 8
    /// This reserves roughly 12.5 % of the memory budget for the buffer pool,
    /// leaving the majority available for in-memory operator data.
    pub fn new(
        mut reader: R,
        mut writer: W,
        block_size: u64,
        memory_limit_mb: u64,
    ) -> Result<Self> {
        // Ask the disk where the writable (anonymous) region begins.
        writer.write_all(b"get anon-start-block\n")?;
        writer.flush()?;

        let mut line = String::new();
        reader.read_line(&mut line)?;
        let anon_start: u64 = line.trim().parse()?;

        // Compute cache capacity: at least 16 blocks, at most 4096 blocks.
        let bs = block_size as usize;
        let cache_capacity = {
            let budget_bytes = (memory_limit_mb as usize) * 1024 * 1024 / 8;
            let cap = budget_bytes / bs;
            cap.max(16).min(4096)
        };

        Ok(Self {
            reader,
            writer,
            block_size: bs,
            anon_start,
            next_anon: anon_start,
            cache: HashMap::with_capacity(cache_capacity + 1),
            lru: VecDeque::with_capacity(cache_capacity + 1),
            cache_capacity,
            free_scratch: Vec::new(),
        })
    }

    // ─── Reading from disk ─────────────────────────────────────────────────────

    /// Read `num_blocks` consecutive blocks starting at `start_block_id`.
    /// The raw bytes are written into `out`, which is resized accordingly.
    ///
    /// Single read-only (file) block reads are served from the LRU cache when
    /// possible; multi-block reads bypass the cache (sequential scan pattern).
    pub fn read_blocks(
        &mut self,
        start_block_id: u64,
        num_blocks: u64,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        // ── Cache path: single file-region block ───────────────────────────
        if num_blocks == 1 && start_block_id < self.anon_start {
            if self.cache.contains_key(&start_block_id) {
                // Cache hit — copy the cached data and update LRU order.
                let cached = self.cache[&start_block_id].clone();
                self.lru_touch(start_block_id);
                out.resize(self.block_size, 0);
                out.copy_from_slice(&cached);
                return Ok(());
            }
        }

        // ── Disk path ──────────────────────────────────────────────────────
        let total_bytes = num_blocks as usize * self.block_size;
        out.resize(total_bytes, 0);

        let cmd = format!("get block {} {}\n", start_block_id, num_blocks);
        self.writer.write_all(cmd.as_bytes())?;
        self.writer.flush()?;
        self.reader.read_exact(out)?;

        // ── Insert single file-region blocks into the cache ────────────────
        if num_blocks == 1 && start_block_id < self.anon_start {
            self.lru_insert(start_block_id, out.clone());
        }

        Ok(())
    }

    /// Ask the disk for the starting block ID of a named file.
    pub fn get_file_start(&mut self, file_id: &str) -> Result<u64> {
        let cmd = format!("get file start-block {}\n", file_id);
        self.writer.write_all(cmd.as_bytes())?;
        self.writer.flush()?;

        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        Ok(line.trim().parse()?)
    }

    /// Ask the disk for the number of blocks a named file spans.
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
    /// Checks the free list first; if a previously freed range of exactly `n`
    /// blocks exists, it is reused.  Otherwise bumps `next_anon`.
    pub fn alloc_scratch(&mut self, n: u64) -> u64 {
        // Look for a free range of at least n blocks; split if larger.
        if let Some(pos) = self.free_scratch.iter().position(|&(_, count)| count >= n) {
            let (start, count) = self.free_scratch.swap_remove(pos);
            if count > n {
                self.free_scratch.push((start + n, count - n));
            }
            return start;
        }
        // Fall back to fresh allocation.
        let start = self.next_anon;
        self.next_anon += n;
        start
    }

    /// Return a scratch range to the free list so it can be reused later.
    ///
    /// Called by the sort's merge phase after two source runs have been
    /// merged into a single output run and the inputs are no longer needed.
    pub fn free_scratch_range(&mut self, start: u64, num_blocks: u64) {
        if num_blocks > 0 {
            self.free_scratch.push((start, num_blocks));
        }
    }

    /// Write `data` to scratch blocks starting at `block_id`.
    ///
    /// Scratch blocks are NOT added to the LRU cache — they are write-once
    /// (used for external sort runs) and reading them later is handled by
    /// RunReader which issues regular disk reads.
    pub fn write_scratch(&mut self, block_id: u64, data: &[u8]) -> Result<()> {
        debug_assert!(
            data.len() % self.block_size == 0,
            "write_scratch: data length must be a multiple of block_size"
        );
        let num_blocks = data.len() / self.block_size;

        let cmd = format!("put block {} {}\n", block_id, num_blocks);
        self.writer.write_all(cmd.as_bytes())?;
        self.writer.write_all(data)?;
        self.writer.flush()?;
        Ok(())
    }

    // ─── LRU helpers (private) ─────────────────────────────────────────────────

    /// Record a cache hit: move `block_id` to the front of the LRU queue.
    fn lru_touch(&mut self, block_id: u64) {
        // Find and remove from current position, then push to front.
        // O(n) scan is acceptable given the small cache size (≤ 4096 entries).
        if let Some(pos) = self.lru.iter().position(|&id| id == block_id) {
            self.lru.remove(pos);
        }
        self.lru.push_front(block_id);
    }

    /// Insert a new block into the cache, evicting the LRU entry if full.
    fn lru_insert(&mut self, block_id: u64, data: Vec<u8>) {
        if self.cache.contains_key(&block_id) {
            // Already cached — just refresh its position.
            self.lru_touch(block_id);
            return;
        }
        // Evict the least-recently-used block(s) until we have room.
        while self.cache.len() >= self.cache_capacity {
            if let Some(evict_id) = self.lru.pop_back() {
                self.cache.remove(&evict_id);
            } else {
                break;
            }
        }
        self.lru.push_front(block_id);
        self.cache.insert(block_id, data);
    }
}
