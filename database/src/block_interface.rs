// Buffer-manager-backed disk interface
// Minimizes I/O by:
//   1. LRU block cache — avoids re-reading blocks already in memory
//   2. Batched disk reads — contiguous uncached blocks fetched in one command
//   3. Metadata cache — file start-block / num-blocks queried at most once per file

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, Write};
use anyhow::{Context, Result};

pub struct BlockInterface<R: BufRead, W: Write> {
    disk_reader: R,
    disk_writer: W,
    block_size: u64,

    // Buffer pool: block_id → block bytes
    buffer_pool: HashMap<u64, Vec<u8>>,
    // LRU order: front = least recently used, back = most recently used
    lru_order: VecDeque<u64>,
    max_frames: usize,

    // Scratch-space tracking
    #[allow(dead_code)]
    anon_start_block: u64,
    #[allow(dead_code)]
    next_free_anon_block: u64,

    // Metadata caches
    file_start_cache: HashMap<String, u64>,
    file_num_blocks_cache: HashMap<String, u64>,
}

impl<R: BufRead, W: Write> BlockInterface<R, W> {
    /// Initialises the interface.  `block_size` was already fetched by main;
    /// we only need to ask the disk for the anonymous-region start block.
    pub fn new(
        mut disk_reader: R,
        mut disk_writer: W,
        block_size: u64,
        memory_limit_mb: u64,
    ) -> Result<Self> {
        disk_writer.write_all(b"get anon-start-block\n")?;
        disk_writer.flush()?;

        let mut line = String::new();
        disk_reader.read_line(&mut line)?;
        let anon_start_block: u64 = line
            .trim()
            .parse()
            .context("Failed to parse anon-start-block")?;

        // Dedicate half of the memory budget to the buffer pool
        let pool_bytes = (memory_limit_mb * 1024 * 1024) / 2;
        let max_frames = ((pool_bytes / block_size) as usize).max(1);

        Ok(Self {
            disk_reader,
            disk_writer,
            block_size,
            buffer_pool: HashMap::new(),
            lru_order: VecDeque::new(),
            max_frames,
            anon_start_block,
            next_free_anon_block: anon_start_block,
            file_start_cache: HashMap::new(),
            file_num_blocks_cache: HashMap::new(),
        })
    }

    pub fn get_block_size(&self) -> u64 {
        self.block_size
    }

    /// Read `num_blocks` consecutive blocks starting at `start_block_id`.
    /// Cached blocks are served from the pool; uncached contiguous runs are
    /// fetched from disk with a single `get block` command each.
    pub fn read_blocks(&mut self, start_block_id: u64, num_blocks: u64) -> Result<Vec<u8>> {
        // ── 1. Find contiguous runs of uncached blocks ──────────────────────
        let mut runs: Vec<(u64, u64)> = Vec::new(); // (start, count)
        let mut run_start: Option<u64> = None;

        for i in 0..num_blocks {
            let block_id = start_block_id + i;
            if self.buffer_pool.contains_key(&block_id) {
                if let Some(start) = run_start.take() {
                    runs.push((start, block_id - start));
                }
            } else if run_start.is_none() {
                run_start = Some(block_id);
            }
        }
        if let Some(start) = run_start {
            runs.push((start, (start_block_id + num_blocks) - start));
        }

        // ── 2. Fetch each uncached run in a single disk command ─────────────
        for (run_start_id, run_count) in runs {
            let cmd = format!("get block {} {}\n", run_start_id, run_count);
            self.disk_writer.write_all(cmd.as_bytes())?;
            self.disk_writer.flush()?;

            let bs = self.block_size as usize;
            for offset in 0..run_count {
                let mut buf = vec![0u8; bs];
                self.disk_reader.read_exact(&mut buf)?;

                let block_id = run_start_id + offset;
                if self.buffer_pool.len() >= self.max_frames {
                    if let Some(victim) = self.lru_order.pop_front() {
                        self.buffer_pool.remove(&victim);
                    }
                }
                self.buffer_pool.insert(block_id, buf);
                self.lru_order.push_back(block_id);
            }
        }

        // ── 3. Assemble result and refresh LRU positions ────────────────────
        let bs = self.block_size as usize;
        let mut result = vec![0u8; num_blocks as usize * bs];

        for i in 0..num_blocks {
            let block_id = start_block_id + i;
            if let Some(pos) = self.lru_order.iter().position(|&b| b == block_id) {
                self.lru_order.remove(pos);
                self.lru_order.push_back(block_id);
            }
            let offset = i as usize * bs;
            result[offset..offset + bs]
                .copy_from_slice(self.buffer_pool.get(&block_id).unwrap());
        }

        Ok(result)
    }

    /// Write `data` into the anonymous scratch region; returns the start block_id.
    #[allow(dead_code)]
    pub fn write_scratch(&mut self, data: &[u8]) -> Result<u64> {
        let num_blocks = (data.len() as u64 + self.block_size - 1) / self.block_size;
        let start_block = self.next_free_anon_block;

        let padded_len = (num_blocks * self.block_size) as usize;
        let mut padded = vec![0u8; padded_len];
        padded[..data.len()].copy_from_slice(data);

        let cmd = format!("put block {} {}\n", start_block, num_blocks);
        self.disk_writer.write_all(cmd.as_bytes())?;
        self.disk_writer.flush()?;
        self.disk_writer.write_all(&padded)?;
        self.disk_writer.flush()?;

        self.next_free_anon_block += num_blocks;
        Ok(start_block)
    }

    /// Return the first block_id of the named file (cached after first query).
    pub fn get_file_start_block(&mut self, file_id: &str) -> Result<u64> {
        if let Some(&v) = self.file_start_cache.get(file_id) {
            return Ok(v);
        }
        let cmd = format!("get file start-block {}\n", file_id);
        self.disk_writer.write_all(cmd.as_bytes())?;
        self.disk_writer.flush()?;

        let mut line = String::new();
        self.disk_reader.read_line(&mut line)?;
        let v: u64 = line
            .trim()
            .parse()
            .context("Failed to parse file start-block")?;
        self.file_start_cache.insert(file_id.to_string(), v);
        Ok(v)
    }

    /// Return the number of blocks the named file occupies (cached after first query).
    pub fn get_file_num_blocks(&mut self, file_id: &str) -> Result<u64> {
        if let Some(&v) = self.file_num_blocks_cache.get(file_id) {
            return Ok(v);
        }
        let cmd = format!("get file num-blocks {}\n", file_id);
        self.disk_writer.write_all(cmd.as_bytes())?;
        self.disk_writer.flush()?;

        let mut line = String::new();
        self.disk_reader.read_line(&mut line)?;
        let v: u64 = line
            .trim()
            .parse()
            .context("Failed to parse file num-blocks")?;
        self.file_num_blocks_cache.insert(file_id.to_string(), v);
        Ok(v)
    }
}
