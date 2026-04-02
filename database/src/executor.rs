// Recursive query-tree executor.
// Binary on-disk block format (written by generator):
//   Each block = [packed rows...][padding...][row_count: u16 LE at last 2 bytes]
//   Row fields in schema order:
//     Int32 / Int64 / Float32 / Float64 → little-endian bytes
//     String → bytes followed by a 0x00 null terminator

use anyhow::{anyhow, bail, Result};
use std::io::{BufRead, Write};

use common::query::{
    ComparisionOperator, ComparisionValue, CrossData, FilterData, Predicate, ProjectData, QueryOp,
    ScanData, SortData,
};
use common::{Data, DataType};
use db_config::DbContext;

use crate::block_interface::BlockInterface;

pub type Row = Vec<(String, Data)>;

// ─── public entry point ──────────────────────────────────────────────────────

pub fn execute<R: BufRead, W: Write>(
    op: &QueryOp,
    ctx: &DbContext,
    bi: &mut BlockInterface<R, W>,
) -> Result<Vec<Row>> {
    match op {
        QueryOp::Scan(d) => exec_scan(d, ctx, bi),
        QueryOp::Filter(d) => exec_filter(d, ctx, bi),
        QueryOp::Project(d) => exec_project(d, ctx, bi),
        QueryOp::Sort(d) => exec_sort(d, ctx, bi),
        QueryOp::Cross(d) => exec_cross(d, ctx, bi),
    }
}

// ─── operators ───────────────────────────────────────────────────────────────

fn exec_scan<R: BufRead, W: Write>(
    data: &ScanData,
    ctx: &DbContext,
    bi: &mut BlockInterface<R, W>,
) -> Result<Vec<Row>> {
    let table_spec = ctx
        .get_table_specs()
        .iter()
        .find(|t| t.name == data.table_id)
        .ok_or_else(|| anyhow!("Unknown table: {}", data.table_id))?;

    let start = bi.get_file_start_block(&table_spec.file_id)?;
    let count = bi.get_file_num_blocks(&table_spec.file_id)?;
    let block_size = bi.get_block_size() as usize;
    let raw = bi.read_blocks(start, count)?;

    parse_rows_binary(&raw, block_size, &table_spec.column_specs)
}

fn exec_filter<R: BufRead, W: Write>(
    data: &FilterData,
    ctx: &DbContext,
    bi: &mut BlockInterface<R, W>,
) -> Result<Vec<Row>> {
    let rows = execute(&data.underlying, ctx, bi)?;
    Ok(rows
        .into_iter()
        .filter(|row| data.predicates.iter().all(|p| evaluate_predicate(row, p)))
        .collect())
}

fn exec_project<R: BufRead, W: Write>(
    data: &ProjectData,
    ctx: &DbContext,
    bi: &mut BlockInterface<R, W>,
) -> Result<Vec<Row>> {
    let rows = execute(&data.underlying, ctx, bi)?;
    Ok(rows
        .into_iter()
        .map(|row| {
            data.column_name_map
                .iter()
                .filter_map(|(from, to)| {
                    row.iter()
                        .find(|(name, _)| name == from)
                        .map(|(_, val)| (to.clone(), val.clone()))
                })
                .collect()
        })
        .collect())
}

fn exec_sort<R: BufRead, W: Write>(
    data: &SortData,
    ctx: &DbContext,
    bi: &mut BlockInterface<R, W>,
) -> Result<Vec<Row>> {
    let mut rows = execute(&data.underlying, ctx, bi)?;

    rows.sort_by(|a, b| {
        for spec in &data.sort_specs {
            let va = a.iter().find(|(n, _)| n == &spec.column_name).map(|(_, v)| v);
            let vb = b.iter().find(|(n, _)| n == &spec.column_name).map(|(_, v)| v);

            let ord = match (va, vb) {
                (Some(av), Some(bv)) => av.partial_cmp(bv).unwrap_or(std::cmp::Ordering::Equal),
                _ => std::cmp::Ordering::Equal,
            };

            let ord = if spec.ascending { ord } else { ord.reverse() };

            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });

    Ok(rows)
}

fn exec_cross<R: BufRead, W: Write>(
    data: &CrossData,
    ctx: &DbContext,
    bi: &mut BlockInterface<R, W>,
) -> Result<Vec<Row>> {
    let left = execute(&data.left, ctx, bi)?;
    let right = execute(&data.right, ctx, bi)?;

    let mut result = Vec::with_capacity(left.len() * right.len());
    for l in &left {
        for r in &right {
            let mut combined = l.clone();
            combined.extend(r.iter().cloned());
            result.push(combined);
        }
    }
    Ok(result)
}

// ─── binary block parser ─────────────────────────────────────────────────────
//
// Block layout (generator: save_row_stream_to_file):
//   bytes [0 .. block_size-2]  — packed rows (zero-padded at end)
//   bytes [block_size-2 .. block_size] — u16 LE row count
//
// A row is NEVER split across blocks.

fn parse_rows_binary(
    data: &[u8],
    block_size: usize,
    col_specs: &[db_config::table::ColumnSpec],
) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    let num_blocks = data.len() / block_size;

    for b in 0..num_blocks {
        let block = &data[b * block_size..(b + 1) * block_size];

        let row_count =
            u16::from_le_bytes([block[block_size - 2], block[block_size - 1]]) as usize;

        if row_count == 0 {
            continue;
        }

        let mut offset = 0usize;
        let map_capacity = block_size - 2;

        for _ in 0..row_count {
            if offset >= map_capacity {
                bail!("Offset exceeded block map capacity while parsing rows");
            }
            let mut row = Row::new();
            for col_spec in col_specs {
                let (value, consumed) =
                    parse_binary_field(&block[offset..map_capacity], &col_spec.data_type)?;
                row.push((col_spec.column_name.clone(), value));
                offset += consumed;
            }
            rows.push(row);
        }
    }

    Ok(rows)
}

fn parse_binary_field(data: &[u8], dt: &DataType) -> Result<(Data, usize)> {
    match dt {
        DataType::Int32 => {
            let bytes: [u8; 4] = data[..4].try_into()?;
            Ok((Data::Int32(i32::from_le_bytes(bytes)), 4))
        }
        DataType::Int64 => {
            let bytes: [u8; 8] = data[..8].try_into()?;
            Ok((Data::Int64(i64::from_le_bytes(bytes)), 8))
        }
        DataType::Float32 => {
            let bytes: [u8; 4] = data[..4].try_into()?;
            Ok((Data::Float32(f32::from_le_bytes(bytes)), 4))
        }
        DataType::Float64 => {
            let bytes: [u8; 8] = data[..8].try_into()?;
            Ok((Data::Float64(f64::from_le_bytes(bytes)), 8))
        }
        DataType::String => {
            let null_pos = data
                .iter()
                .position(|&b| b == 0)
                .ok_or_else(|| anyhow!("String field missing null terminator"))?;
            let s = String::from_utf8_lossy(&data[..null_pos]).into_owned();
            Ok((Data::String(s), null_pos + 1))
        }
    }
}

// ─── predicate evaluation ────────────────────────────────────────────────────

fn evaluate_predicate(row: &Row, pred: &Predicate) -> bool {
    let lhs = match row.iter().find(|(n, _)| n == &pred.column_name) {
        Some((_, v)) => v,
        None => return false,
    };

    match &pred.value {
        ComparisionValue::Column(col) => match row.iter().find(|(n, _)| n == col) {
            Some((_, v)) => apply_op(lhs, &pred.operator, v),
            None => false,
        },
        ComparisionValue::I32(v) => apply_op(lhs, &pred.operator, &Data::Int32(*v)),
        ComparisionValue::I64(v) => apply_op(lhs, &pred.operator, &Data::Int64(*v)),
        ComparisionValue::F32(v) => apply_op(lhs, &pred.operator, &Data::Float32(*v)),
        ComparisionValue::F64(v) => apply_op(lhs, &pred.operator, &Data::Float64(*v)),
        ComparisionValue::String(v) => apply_op(lhs, &pred.operator, &Data::String(v.clone())),
    }
}

fn apply_op(lhs: &Data, op: &ComparisionOperator, rhs: &Data) -> bool {
    use std::cmp::Ordering::*;
    match op {
        ComparisionOperator::EQ => lhs == rhs,
        ComparisionOperator::NE => lhs != rhs,
        ComparisionOperator::GT => lhs.partial_cmp(rhs) == Some(Greater),
        ComparisionOperator::GTE => matches!(lhs.partial_cmp(rhs), Some(Greater) | Some(Equal)),
        ComparisionOperator::LT => lhs.partial_cmp(rhs) == Some(Less),
        ComparisionOperator::LTE => matches!(lhs.partial_cmp(rhs), Some(Less) | Some(Equal)),
    }
}
