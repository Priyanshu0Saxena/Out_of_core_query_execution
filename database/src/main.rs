/// main.rs — Database Entry Point
///
/// Orchestrates the three phases of query processing:
///
///   1. I/O Setup      — open file descriptors to monitor (FD 5/6) and disk (FD 3/4)
///   2. Plan Building  — parse query JSON and build a RowIter execution tree
///   3. Execution      — drive the RowIter tree, streaming results to the monitor
///
/// File descriptor map (set up by the monitor at startup):
///   FD 3 (read)  ← disk simulator  (block data)
///   FD 4 (write) → disk simulator  (commands)
///   FD 5 (read)  ← monitor         (query JSON)
///   FD 6 (write) → monitor         (result rows)
use anyhow::{Context, Result};
use clap::Parser;
use common::query::Query;
use db_config::DbContext;
use std::io::{BufRead, BufReader, Write};

use crate::{
    block_interface::BlockInterface,
    cli::CliOptions,
    executor::build_plan,
    io_setup::{setup_disk_io, setup_monitor_io},
    output::stream_results,
};

mod block_interface;
mod cli;
mod executor;
mod io_setup;
mod output;

fn db_main() -> Result<()> {
    // ── Step 1: Load table metadata from the config file ─────────────────────
    let cli_options = CliOptions::parse();
    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;

    // ── Step 2: Set up I/O channels ───────────────────────────────────────────
    //   disk_in  / disk_out   → FD 3/4 (disk simulator)
    //   monitor_in / monitor_out → FD 5/6 (monitor)
    let (disk_in, mut disk_out) = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();

    // Wrap disk_in in a BufReader for line-by-line text reads
    let mut disk_reader = BufReader::new(disk_in);
    let mut monitor_reader = BufReader::new(monitor_in);

    let mut line = String::new();

    // ── Step 3: Read the query JSON from the monitor (FD 5) ──────────────────
    monitor_reader.read_line(&mut line)?;
    let query: Query =
        serde_json::from_str(&line).context("Failed to parse query JSON from monitor")?;

    // ── Step 4: Get the disk block size (needed before creating BlockInterface) ─
    disk_out.write_all(b"get block-size\n")?;
    disk_out.flush()?;
    line.clear();
    disk_reader.read_line(&mut line)?;
    let block_size: u64 = line
        .trim()
        .parse()
        .context("Failed to parse block-size from disk")?;

    // ── Step 5: Get the memory limit from the monitor ─────────────────────────
    monitor_out.write_all(b"get_memory_limit\n")?;
    monitor_out.flush()?;
    line.clear();
    monitor_reader.read_line(&mut line)?;
    let memory_limit_mb: u64 = line
        .trim()
        .parse()
        .context("Failed to parse memory_limit_mb from monitor")?;

    // ── Step 6: Create the BlockInterface ────────────────────────────────────
    // BlockInterface takes ownership of disk_reader and disk_out.
    // It also queries the disk for anon-start-block internally.
    let mut bi = BlockInterface::new(disk_reader, disk_out, block_size, memory_limit_mb)?;

    // ── Step 7: Build the execution plan tree (no disk I/O yet) ──────────────
    let mut plan = build_plan(&query.root, &ctx)?;

    // ── Step 8: Execute and stream results to the monitor ─────────────────────
    stream_results(&mut plan, &mut bi, &mut monitor_out)?;

    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "Database fatal error")
}
