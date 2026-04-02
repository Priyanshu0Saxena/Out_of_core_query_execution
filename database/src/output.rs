// Format and send result rows to the monitor.
// Protocol: "validate\n"  →  one "v1|v2|...|vN|\n" per row  →  "!\n"

use anyhow::Result;
use std::io::Write;
use common::Data;
use crate::executor::Row;

/// Send all result rows to the monitor in the expected wire format.
pub fn send_results(rows: Vec<Row>, monitor_out: &mut impl Write) -> Result<()> {
    monitor_out.write_all(b"validate\n")?;
    monitor_out.flush()?;

    for row in rows {
        let mut line = String::new();
        for (_, value) in &row {
            line.push_str(&format_value(value));
            line.push('|');
        }
        line.push('\n');
        monitor_out.write_all(line.as_bytes())?;
    }

    monitor_out.write_all(b"!\n")?;
    monitor_out.flush()?;
    Ok(())
}

fn format_value(value: &Data) -> String {
    match value {
        Data::Int32(v) => v.to_string(),
        Data::Int64(v) => v.to_string(),
        Data::Float32(v) => v.to_string(),
        Data::Float64(v) => v.to_string(),
        Data::String(v) => v.clone(),
    }
}
