// Format and send results to monitor
// Take final rows and send them to monitor in correct format
use anyhow::Result;
use std::io::Write;
use common::Data;
use crate::executor::Row;

/// Sends all result rows to monitor in correct format
pub fn send_results(rows: Vec<Row>, monitor_out: &mut impl Write) -> Result<()> {
    // Send validate signal
    // For each row send col1|col2|col3|\n
    // Send ! to signal end
    todo!()
}

/// Format a single Data value as a string
fn format_value(value: &Data) -> String {
    todo!()
}