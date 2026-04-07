/// output.rs — Result Formatter and Monitor Stream Writer
///
/// Role: drive the top-level RowIter to completion and write each row to
/// the monitor in the required wire format.
///
/// Monitor output protocol:
///   1. Send "validate\n"            — signals that results are coming
///   2. For each row:  "v1|v2|...|vN|\n"   — pipe-delimited, trailing pipe
///   3. Send "!\n"                   — signals end of results
///
/// Float formatting: match SQLite's output.
///   SQLite uses the C `%g` format which strips trailing zeros and shows
///   at least one decimal place for whole numbers (e.g. 17.0 not 17).
use std::io::{BufRead, Write};

use anyhow::Result;
use common::Data;

use crate::block_interface::BlockInterface;
use crate::executor::RowIter;

/// Stream all rows from `plan` to `monitor_out`, following the wire protocol.
///
/// Rows are produced and sent one at a time — no need to hold all rows in memory
/// here (though Sort internally buffers everything before yielding its first row).
pub fn stream_results<R: BufRead, W: Write>(
    plan: &mut RowIter,
    bi: &mut BlockInterface<R, W>,
    monitor_out: &mut impl Write,
) -> Result<()> {
    // Signal the monitor that output is starting
    monitor_out.write_all(b"validate\n")?;
    monitor_out.flush()?;

    // Pull and send rows one at a time
    while let Some(row) = plan.next_row(bi)? {
        // Build the row line: "v1|v2|...|vN|\n"
        let mut line = String::new();
        for value in &row {
            line.push_str(&format_value(value));
            line.push('|');
        }
        line.push('\n');
        monitor_out.write_all(line.as_bytes())?;
    }

    // Signal end of output
    monitor_out.write_all(b"!\n")?;
    monitor_out.flush()?;

    Ok(())
}

/// Format one Data value as a string for the output wire format.
fn format_value(value: &Data) -> String {
    match value {
        Data::Int32(v) => v.to_string(),
        Data::Int64(v) => v.to_string(),
        Data::Float32(v) => format_float_f32(*v),
        Data::Float64(v) => format_float_f64(*v),
        Data::String(s) => s.clone(),
    }
}

/// Format a f32 to match SQLite's output.
/// SQLite uses %g style: no trailing zeros, but always shows a decimal point.
fn format_float_f32(v: f32) -> String {
    format_float_f64(v as f64)
}

/// Format a f64 to match SQLite's output.
///
/// Rules (approximating SQLite's %g behaviour):
///   - No trailing zeros after the decimal point
///   - If the value is a whole number, append ".0"
///   - Special values: NaN → "nan", +Inf → "inf", -Inf → "-inf"
fn format_float_f64(v: f64) -> String {
    if v.is_nan() {
        return "nan".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            "inf".to_string()
        } else {
            "-inf".to_string()
        };
    }

    // Rust's default Display for floats gives the shortest round-trip representation.
    // For example: 711.56, 17.0 (wait — actually Rust formats 17.0f64 as "17").
    // We need to add ".0" when there is no decimal point.
    let s = format!("{}", v);

    if s.contains('.') || s.contains('e') || s.contains('E') {
        // Already has a decimal point or exponent — fine as-is
        s
    } else {
        // Whole number like "17" — append ".0" to match SQLite
        format!("{}.0", s)
    }
}
