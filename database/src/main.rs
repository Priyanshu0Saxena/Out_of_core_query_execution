use anyhow::{Context, Result};
use clap::Parser;
use common::query::Query;
use db_config::DbContext;
use std::io::{BufRead, BufReader, Write};

use crate::{
    cli::CliOptions,
    io_setup::{setup_disk_io, setup_monitor_io},
    block_interface::BlockInterface,
    executor::execute,
    output::send_results,
};

mod cli;
mod io_setup;
mod block_interface;
mod executor;
mod output;

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    // Load table/column metadata
    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;

    // Setups and provides handler to talk with disk and monitor
    let (disk_in, mut disk_out) = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();

    // Use buffered reader to read lines easier
    let mut disk_buf_reader = BufReader::new(disk_in);
    let mut monitor_buf_reader = BufReader::new(monitor_in);

    // Temporary variable to read a line of input
    let mut input_line = String::new();

    // Read query from monitor
    monitor_buf_reader.read_line(&mut input_line)?;
    let query: Query = serde_json::from_str(&input_line).unwrap();

    // Get block size from disk
    disk_out.write_all("get block-size\n".as_bytes())?;
    disk_out.flush()?;

    input_line.clear();
    disk_buf_reader.read_line(&mut input_line)?;
    let block_size: u64 = input_line.trim().parse()?;

    // Get memory limit from monitor
    input_line.clear();
    monitor_out.write_all("get_memory_limit\n".as_bytes())?;
    monitor_out.flush()?;
    monitor_buf_reader.read_line(&mut input_line)?;
    let memory_limit_mb: u32 = input_line.trim().parse()?;

    // Create block interface using already fetched block_size
    let mut block_interface = BlockInterface::new(
        &mut disk_buf_reader,
        &mut disk_out,
        block_size,
        memory_limit_mb as u64,
    )?;

    // Execute the query tree recursively
    let result_rows = execute(
        &query.root,
        &ctx,
        &mut block_interface,
    )?;

    // Send results to monitor
    send_results(result_rows, &mut monitor_out)?;

    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "From Database")
}
