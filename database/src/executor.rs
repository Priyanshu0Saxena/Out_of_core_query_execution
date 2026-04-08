/// executor.rs — Query Execution Engine (Volcano / Iterator Model)
///
/// Role: execute a QueryOp tree and produce rows one at a time.
///
/// ┌────────────────────────────────────────────────────┐
/// │  Volcano Model: each operator is an iterator.      │
/// │  The top-level caller drives execution by          │
/// │  repeatedly calling next_row() on the root.        │
/// └────────────────────────────────────────────────────┘
///
/// Operator summary:
///   Scan    — reads rows from a disk table file, one block at a time
///   Filter  — passes only rows satisfying all predicates (AND logic)
///   Project — selects and renames a subset of columns
///   Sort    — external merge sort: safe for data larger than memory
///   Cross   — materializes right child in memory; nested-loop join with left
///
/// External Sort Overview (SortIter):
///   Phase 1 (Collection): pull rows from child in SORT_CHUNK_MEMORY chunks.
///     Sort each chunk in memory. Write each sorted chunk as a "run" to
///     scratch disk using the disk simulator's anonymous block region.
///   Phase 2 (Merge): merge runs pairwise until one sorted run remains.
///     Uses a RunReader for each input run and a RunWriter for output.
///     Only one row from each run is held in memory at a time.
///   Phase 3 (Streaming): yield rows from the final run one at a time.
use std::cmp::Ordering;
use std::io::{BufRead, Write};

use anyhow::{anyhow, Result};
use common::query::{ComparisionOperator, ComparisionValue, Predicate, QueryOp, SortSpec};
use common::{Data, DataType};
use db_config::DbContext;

use crate::block_interface::BlockInterface;

/// A row is an ordered list of column values.
pub type Row = Vec<Data>;
/// A schema is the ordered list of column names for a row.
pub type Schema = Vec<String>;

// ═══════════════════════════════════════════════════════════════════════════
// RowIter — the unified operator enum
// ═══════════════════════════════════════════════════════════════════════════

pub enum RowIter {
    Scan(ScanIter),
    Filter(Box<FilterIter>),
    Project(Box<ProjectIter>),
    Sort(Box<SortIter>),
    Cross(Box<CrossIter>),
}

impl RowIter {
    /// Returns the output column names of this operator.
    pub fn schema(&self) -> &[String] {
        match self {
            RowIter::Scan(s) => &s.schema,
            RowIter::Filter(f) => &f.schema,
            RowIter::Project(p) => &p.out_schema,
            RowIter::Sort(s) => &s.schema,
            RowIter::Cross(c) => &c.schema,
        }
    }

    /// Pull the next row. Returns Ok(None) when exhausted.
    pub fn next_row<R: BufRead, W: Write>(
        &mut self,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<Option<Row>> {
        match self {
            RowIter::Scan(s) => s.next_row(bi),
            RowIter::Filter(f) => f.next_row(bi),
            RowIter::Project(p) => p.next_row(bi),
            RowIter::Sort(s) => s.next_row(bi),
            RowIter::Cross(c) => c.next_row(bi),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SCAN
// ═══════════════════════════════════════════════════════════════════════════

/// ScanIter streams rows from a disk table file, one block at a time.
/// Memory usage: O(block_size).
pub struct ScanIter {
    pub schema: Schema,
    col_types: Vec<DataType>,
    file_id: String,
    // File location — fetched from disk on first call
    start_block: u64,
    num_blocks: u64,
    initialized: bool,
    // Current block state
    current_block_idx: u64,
    block_buf: Vec<u8>,
    rows_in_block: usize,
    row_cursor: usize,
    byte_cursor: usize,
    done: bool,
}

impl ScanIter {
    fn new(schema: Schema, col_types: Vec<DataType>, file_id: String) -> Self {
        Self {
            schema,
            col_types,
            file_id,
            start_block: 0,
            num_blocks: 0,
            initialized: false,
            current_block_idx: 0,
            block_buf: Vec::new(),
            rows_in_block: 0,
            row_cursor: 0,
            byte_cursor: 0,
            done: false,
        }
    }

    fn next_row<R: BufRead, W: Write>(
        &mut self,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<Option<Row>> {
        if !self.initialized {
            self.start_block = bi.get_file_start(&self.file_id)?;
            self.num_blocks = bi.get_file_num_blocks(&self.file_id)?;
            self.initialized = true;
            if self.num_blocks == 0 {
                self.done = true;
                return Ok(None);
            }
            self.load_block(bi)?;
        }
        if self.done {
            return Ok(None);
        }
        // Advance to next block when current block is exhausted
        while self.row_cursor >= self.rows_in_block {
            self.current_block_idx += 1;
            if self.current_block_idx >= self.num_blocks {
                self.done = true;
                return Ok(None);
            }
            self.load_block(bi)?;
        }
        let row = decode_row(&self.block_buf, &mut self.byte_cursor, &self.col_types)?;
        self.row_cursor += 1;
        Ok(Some(row))
    }

    fn load_block<R: BufRead, W: Write>(&mut self, bi: &mut BlockInterface<R, W>) -> Result<()> {
        let block_id = self.start_block + self.current_block_idx;
        bi.read_blocks(block_id, 1, &mut self.block_buf)?;
        let bs = bi.block_size;
        // Row count is stored in the last 2 bytes as u16 little-endian
        self.rows_in_block =
            (self.block_buf[bs - 2] as usize) | ((self.block_buf[bs - 1] as usize) << 8);
        self.row_cursor = 0;
        self.byte_cursor = 0;
        Ok(())
    }
}

/// Decode one row from a block buffer, advancing `cursor`.
fn decode_row(block: &[u8], cursor: &mut usize, col_types: &[DataType]) -> Result<Row> {
    let mut row = Vec::with_capacity(col_types.len());
    for col_type in col_types {
        let value = match col_type {
            DataType::Int32 => {
                let b: [u8; 4] = block[*cursor..*cursor + 4].try_into()?;
                *cursor += 4;
                Data::Int32(i32::from_le_bytes(b))
            }
            DataType::Int64 => {
                let b: [u8; 8] = block[*cursor..*cursor + 8].try_into()?;
                *cursor += 8;
                Data::Int64(i64::from_le_bytes(b))
            }
            DataType::Float32 => {
                let b: [u8; 4] = block[*cursor..*cursor + 4].try_into()?;
                *cursor += 4;
                Data::Float32(f32::from_le_bytes(b))
            }
            DataType::Float64 => {
                let b: [u8; 8] = block[*cursor..*cursor + 8].try_into()?;
                *cursor += 8;
                Data::Float64(f64::from_le_bytes(b))
            }
            DataType::String => {
                let start = *cursor;
                while *cursor < block.len() && block[*cursor] != 0 {
                    *cursor += 1;
                }
                let s = std::str::from_utf8(&block[start..*cursor])
                    .map_err(|e| anyhow!("UTF-8 error in scan: {}", e))?
                    .to_owned();
                *cursor += 1; // skip null terminator
                Data::String(s)
            }
        };
        row.push(value);
    }
    Ok(row)
}

// ═══════════════════════════════════════════════════════════════════════════
// FILTER
// ═══════════════════════════════════════════════════════════════════════════

pub struct FilterIter {
    pub schema: Schema,
    predicates: Vec<Predicate>,
    child: RowIter,
}

impl FilterIter {
    fn next_row<R: BufRead, W: Write>(
        &mut self,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<Option<Row>> {
        loop {
            match self.child.next_row(bi)? {
                None => return Ok(None),
                Some(row) => {
                    if row_passes_all_predicates(&row, &self.predicates, &self.schema) {
                        return Ok(Some(row));
                    }
                }
            }
        }
    }
}

fn row_passes_all_predicates(row: &Row, predicates: &[Predicate], schema: &[String]) -> bool {
    predicates.iter().all(|p| evaluate_predicate(row, p, schema))
}

fn evaluate_predicate(row: &Row, pred: &Predicate, schema: &[String]) -> bool {
    let lhs = &row[find_column_index(&pred.column_name, schema)];
    let rhs: Data = match &pred.value {
        ComparisionValue::Column(col) => row[find_column_index(col, schema)].clone(),
        ComparisionValue::I32(v) => Data::Int32(*v),
        ComparisionValue::I64(v) => Data::Int64(*v),
        ComparisionValue::F32(v) => Data::Float32(*v),
        ComparisionValue::F64(v) => Data::Float64(*v),
        ComparisionValue::String(s) => Data::String(s.clone()),
    };
    let cmp = compare_values(lhs, &rhs);
    match pred.operator {
        ComparisionOperator::EQ => cmp == Some(Ordering::Equal),
        ComparisionOperator::NE => cmp != Some(Ordering::Equal),
        ComparisionOperator::GT => cmp == Some(Ordering::Greater),
        ComparisionOperator::GTE => matches!(cmp, Some(Ordering::Greater) | Some(Ordering::Equal)),
        ComparisionOperator::LT => cmp == Some(Ordering::Less),
        ComparisionOperator::LTE => matches!(cmp, Some(Ordering::Less) | Some(Ordering::Equal)),
    }
}

/// Compare two Data values, coercing mismatched numeric types to f64.
fn compare_values(lhs: &Data, rhs: &Data) -> Option<Ordering> {
    // Fast path: same type
    let direct = lhs.partial_cmp(rhs);
    if direct.is_some() {
        return direct;
    }
    // Cross-type numeric comparison
    match (data_to_f64(lhs), data_to_f64(rhs)) {
        (Some(l), Some(r)) => l.partial_cmp(&r),
        _ => None,
    }
}

fn data_to_f64(val: &Data) -> Option<f64> {
    match val {
        Data::Int32(v) => Some(*v as f64),
        Data::Int64(v) => Some(*v as f64),
        Data::Float32(v) => Some(*v as f64),
        Data::Float64(v) => Some(*v),
        Data::String(_) => None,
    }
}

fn find_column_index(col_name: &str, schema: &[String]) -> usize {
    schema
        .iter()
        .position(|n| n == col_name)
        .unwrap_or_else(|| panic!("Column '{}' not in schema {:?}", col_name, schema))
}

// ═══════════════════════════════════════════════════════════════════════════
// PROJECT
// ═══════════════════════════════════════════════════════════════════════════

pub struct ProjectIter {
    pub out_schema: Schema,
    col_indices: Vec<usize>,
    child: RowIter,
}

impl ProjectIter {
    fn next_row<R: BufRead, W: Write>(
        &mut self,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<Option<Row>> {
        match self.child.next_row(bi)? {
            None => Ok(None),
            Some(child_row) => Ok(Some(
                self.col_indices.iter().map(|&i| child_row[i].clone()).collect(),
            )),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SORT — external merge sort
// ═══════════════════════════════════════════════════════════════════════════

/// Memory budget for in-memory sort chunks. Keeps us well within 64 MB.
const SORT_CHUNK_MEMORY: usize = 20 * 1024 * 1024; // 20 MB

/// Metadata describing a sorted run stored on scratch disk.
#[derive(Clone)]
struct RunMeta {
    start_block: u64,
    num_blocks: u64,
}

/// SortIter uses external merge sort so it can handle data larger than memory.
///
/// Phase 0 — Collection: pull rows from child in chunks ≤ SORT_CHUNK_MEMORY,
///   sort each chunk, write as a run to scratch disk.
/// Phase 1 — Merge: merge runs pairwise on disk until one remains.
/// Phase 2 — Stream: yield rows from the final run one at a time.
pub struct SortIter {
    pub schema: Schema,
    sort_specs: Vec<SortSpec>,
    // Set to None after collection starts
    child: Option<Box<RowIter>>,

    // Collection state
    col_types: Vec<DataType>,
    chunk: Vec<Row>,
    chunk_mem: usize,
    runs: Vec<RunMeta>,

    // Streaming state (set after merge)
    run_reader: Option<RunReader>,

    // 0 = collecting, 1 = streaming, 2 = done
    phase: u8,
}

impl SortIter {
    fn next_row<R: BufRead, W: Write>(
        &mut self,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<Option<Row>> {
        // Phase 0: collect all rows into sorted runs, then merge
        if self.phase == 0 {
            self.collect_phase(bi)?;
            self.merge_phase(bi)?;
            self.phase = 1;
        }

        if self.phase != 1 {
            return Ok(None);
        }

        // Phase 1: stream from the single merged run
        let result = match &mut self.run_reader {
            None => Ok(None),
            Some(reader) => reader.next_row(bi),
        };
        if matches!(result, Ok(None)) {
            self.phase = 2;
        }
        result
    }

    /// Drain the child operator, creating sorted runs on scratch disk.
    fn collect_phase<R: BufRead, W: Write>(
        &mut self,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<()> {
        let mut child = self.child.take().expect("child must be Some during collection");
        while let Some(row) = child.next_row(bi)? {
            // Infer column types from the first row we see
            if self.col_types.is_empty() {
                self.col_types = infer_col_types(&row);
            }
            self.chunk_mem += estimate_row_memory(&row);
            self.chunk.push(row);

            if self.chunk_mem >= SORT_CHUNK_MEMORY {
                self.flush_chunk(bi)?;
            }
        }
        // Flush whatever remains in the chunk buffer
        if !self.chunk.is_empty() {
            self.flush_chunk(bi)?;
        }
        Ok(())
    }

    /// Sort the current chunk and write it as a run to scratch disk.
    fn flush_chunk<R: BufRead, W: Write>(
        &mut self,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<()> {
        let schema = self.schema.clone();
        let sort_specs = self.sort_specs.clone();
        self.chunk
            .sort_by(|a, b| compare_rows(a, b, &sort_specs, &schema));
        let run = write_rows_to_scratch(&self.chunk, bi)?;
        self.runs.push(run);
        self.chunk.clear();
        self.chunk_mem = 0;
        Ok(())
    }

    /// Pairwise-merge all runs until one final sorted run remains.
    fn merge_phase<R: BufRead, W: Write>(
        &mut self,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<()> {
        if self.runs.is_empty() {
            self.run_reader = None;
            return Ok(());
        }
        // Repeatedly merge pairs until only one run remains
        while self.runs.len() > 1 {
            let mut merged_runs = Vec::new();
            let mut i = 0;
            while i < self.runs.len() {
                if i + 1 < self.runs.len() {
                    let m = merge_two_runs(
                        &self.runs[i].clone(),
                        &self.runs[i + 1].clone(),
                        &self.col_types,
                        &self.sort_specs,
                        &self.schema,
                        bi,
                    )?;
                    merged_runs.push(m);
                    i += 2;
                } else {
                    // Odd run passes through unchanged
                    merged_runs.push(self.runs[i].clone());
                    i += 1;
                }
            }
            self.runs = merged_runs;
        }
        // Set up a reader for the single final run
        let block_size = bi.block_size;
        self.run_reader = Some(RunReader::new(
            self.runs[0].clone(),
            self.col_types.clone(),
            block_size,
        ));
        Ok(())
    }
}

// ─── External sort helpers ────────────────────────────────────────────────

/// Infer DataType for each column from a row's actual values.
fn infer_col_types(row: &Row) -> Vec<DataType> {
    row.iter()
        .map(|val| match val {
            Data::Int32(_) => DataType::Int32,
            Data::Int64(_) => DataType::Int64,
            Data::Float32(_) => DataType::Float32,
            Data::Float64(_) => DataType::Float64,
            Data::String(_) => DataType::String,
        })
        .collect()
}

/// Estimate how much memory a row occupies in RAM.
/// Used to decide when a chunk has hit the memory budget.
fn estimate_row_memory(row: &Row) -> usize {
    // Vec<Data> struct: 24 bytes
    // Each Data enum: ~32 bytes (covers the String variant's 24-byte String struct)
    // String data payload: actual string length
    let base = 24 + row.len() * 32;
    let str_payload: usize = row
        .iter()
        .filter_map(|v| if let Data::String(s) = v { Some(s.len()) } else { None })
        .sum();
    base + str_payload
}

/// How many bytes a row occupies when encoded on disk (matches `encode_row_into`).
fn encoded_row_len(row: &Row) -> usize {
    row.iter()
        .map(|val| match val {
            Data::Int32(_) => 4,
            Data::Int64(_) => 8,
            Data::Float32(_) => 4,
            Data::Float64(_) => 8,
            Data::String(s) => s.len() + 1, // payload + null terminator
        })
        .sum()
}

/// Encode a row into a pre-sized byte slice.
/// Encoding matches the disk file format (same as the table binary files).
fn encode_row_into(row: &Row, buf: &mut [u8]) {
    let mut pos = 0;
    for val in row {
        match val {
            Data::Int32(v) => {
                buf[pos..pos + 4].copy_from_slice(&v.to_le_bytes());
                pos += 4;
            }
            Data::Int64(v) => {
                buf[pos..pos + 8].copy_from_slice(&v.to_le_bytes());
                pos += 8;
            }
            Data::Float32(v) => {
                buf[pos..pos + 4].copy_from_slice(&v.to_le_bytes());
                pos += 4;
            }
            Data::Float64(v) => {
                buf[pos..pos + 8].copy_from_slice(&v.to_le_bytes());
                pos += 8;
            }
            Data::String(s) => {
                let slen = s.len();
                buf[pos..pos + slen].copy_from_slice(s.as_bytes());
                buf[pos + slen] = 0; // null terminator
                pos += slen + 1;
            }
        }
    }
}

/// Compare two rows using the given sort specifications.
fn compare_rows(a: &Row, b: &Row, sort_specs: &[SortSpec], schema: &[String]) -> Ordering {
    for spec in sort_specs {
        let idx = find_column_index(&spec.column_name, schema);
        let ord = compare_values(&a[idx], &b[idx]).unwrap_or(Ordering::Equal);
        let ord = if spec.ascending { ord } else { ord.reverse() };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

// ─── RunWriter ────────────────────────────────────────────────────────────

/// Writes rows into scratch disk blocks, one block at a time.
/// Blocks use the same layout as disk table files:
///   [row1 bytes][row2 bytes]...[padding][row_count_low][row_count_high]
struct RunWriter {
    block_size: usize,
    start_block: Option<u64>, // set when the first block is flushed
    blocks_written: u64,
    current_block: Vec<u8>,
    byte_pos: usize, // bytes used so far in current_block (not counting 2-byte footer)
    rows_in_current: u16,
}

impl RunWriter {
    fn new(block_size: usize) -> Self {
        Self {
            block_size,
            start_block: None,
            blocks_written: 0,
            current_block: vec![0u8; block_size],
            byte_pos: 0,
            rows_in_current: 0,
        }
    }

    /// Add a row to the current block, flushing if there is no room.
    fn push_row<R: BufRead, W: Write>(
        &mut self,
        row: &Row,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<()> {
        let row_len = encoded_row_len(row);
        let usable = self.block_size - 2;
        debug_assert!(
            row_len <= usable,
            "Single row ({}B) exceeds usable block space ({}B)",
            row_len,
            usable
        );
        if self.byte_pos + row_len > usable {
            self.flush_block(bi)?;
        }
        encode_row_into(row, &mut self.current_block[self.byte_pos..self.byte_pos + row_len]);
        self.byte_pos += row_len;
        self.rows_in_current += 1;
        Ok(())
    }

    /// Write the current block to scratch disk and reset state.
    fn flush_block<R: BufRead, W: Write>(
        &mut self,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<()> {
        if self.rows_in_current == 0 {
            return Ok(());
        }
        let bs = self.block_size;
        self.current_block[bs - 2] = (self.rows_in_current & 0xFF) as u8;
        self.current_block[bs - 1] = ((self.rows_in_current >> 8) & 0xFF) as u8;

        let block_id = bi.alloc_scratch(1);
        if self.start_block.is_none() {
            self.start_block = Some(block_id);
        }
        bi.write_scratch(block_id, &self.current_block)?;
        self.blocks_written += 1;

        // Reset for the next block
        self.current_block.fill(0);
        self.byte_pos = 0;
        self.rows_in_current = 0;
        Ok(())
    }

    /// Flush any remaining rows and return the RunMeta describing the written run.
    fn finish<R: BufRead, W: Write>(
        mut self,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<RunMeta> {
        self.flush_block(bi)?;
        Ok(RunMeta {
            start_block: self.start_block.unwrap_or(0),
            num_blocks: self.blocks_written,
        })
    }
}

/// Write a slice of rows to scratch disk. Returns a RunMeta describing the run.
fn write_rows_to_scratch<R: BufRead, W: Write>(
    rows: &[Row],
    bi: &mut BlockInterface<R, W>,
) -> Result<RunMeta> {
    if rows.is_empty() {
        return Ok(RunMeta {
            start_block: 0,
            num_blocks: 0,
        });
    }
    let mut writer = RunWriter::new(bi.block_size);
    for row in rows {
        writer.push_row(row, bi)?;
    }
    writer.finish(bi)
}

// ─── RunReader ────────────────────────────────────────────────────────────

/// Reads rows sequentially from a sorted run on scratch disk.
struct RunReader {
    run: RunMeta,
    col_types: Vec<DataType>,
    block_offset: u64,  // 0-based index within the run
    block_buf: Vec<u8>, // holds the currently-loaded block
    rows_in_block: usize,
    row_cursor: usize,
    byte_cursor: usize,
    initialized: bool,
    done: bool,
}

impl RunReader {
    fn new(run: RunMeta, col_types: Vec<DataType>, block_size: usize) -> Self {
        let done = run.num_blocks == 0;
        Self {
            run,
            col_types,
            block_offset: 0,
            block_buf: vec![0u8; block_size],
            rows_in_block: 0,
            row_cursor: 0,
            byte_cursor: 0,
            initialized: false,
            done,
        }
    }

    fn next_row<R: BufRead, W: Write>(
        &mut self,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<Option<Row>> {
        if self.done {
            return Ok(None);
        }
        // Load the first block on first call
        if !self.initialized {
            self.load_block(bi)?;
            self.initialized = true;
        }
        // Advance to next block when this one is exhausted
        while self.row_cursor >= self.rows_in_block {
            self.block_offset += 1;
            if self.block_offset >= self.run.num_blocks {
                self.done = true;
                return Ok(None);
            }
            self.load_block(bi)?;
        }
        let row = decode_row(&self.block_buf, &mut self.byte_cursor, &self.col_types)?;
        self.row_cursor += 1;
        Ok(Some(row))
    }

    fn load_block<R: BufRead, W: Write>(&mut self, bi: &mut BlockInterface<R, W>) -> Result<()> {
        let block_id = self.run.start_block + self.block_offset;
        bi.read_blocks(block_id, 1, &mut self.block_buf)?;
        let bs = bi.block_size;
        self.rows_in_block =
            (self.block_buf[bs - 2] as usize) | ((self.block_buf[bs - 1] as usize) << 8);
        self.row_cursor = 0;
        self.byte_cursor = 0;
        Ok(())
    }
}

// ─── 2-way merge ─────────────────────────────────────────────────────────

/// Merge two sorted runs into a new sorted run on scratch disk.
/// Memory usage: O(block_size) per input run + O(block_size) output buffer.
fn merge_two_runs<R: BufRead, W: Write>(
    run_a: &RunMeta,
    run_b: &RunMeta,
    col_types: &[DataType],
    sort_specs: &[SortSpec],
    schema: &[String],
    bi: &mut BlockInterface<R, W>,
) -> Result<RunMeta> {
    let block_size = bi.block_size;
    let mut reader_a = RunReader::new(run_a.clone(), col_types.to_vec(), block_size);
    let mut reader_b = RunReader::new(run_b.clone(), col_types.to_vec(), block_size);
    let mut writer = RunWriter::new(block_size);

    // Prime both readers with their first row
    let mut opt_a = reader_a.next_row(bi)?;
    let mut opt_b = reader_b.next_row(bi)?;

    loop {
        match (opt_a.is_some(), opt_b.is_some()) {
            (false, false) => break,
            // One side exhausted — drain the other
            (true, false) => {
                writer.push_row(opt_a.as_ref().unwrap(), bi)?;
                opt_a = reader_a.next_row(bi)?;
            }
            (false, true) => {
                writer.push_row(opt_b.as_ref().unwrap(), bi)?;
                opt_b = reader_b.next_row(bi)?;
            }
            // Both sides have a row — pick the smaller one
            (true, true) => {
                // Compare inside a block so the borrows end before we mutate opt_a/opt_b
                let take_a = {
                    let a = opt_a.as_ref().unwrap();
                    let b = opt_b.as_ref().unwrap();
                    compare_rows(a, b, sort_specs, schema) != Ordering::Greater
                };
                if take_a {
                    writer.push_row(opt_a.as_ref().unwrap(), bi)?;
                    opt_a = reader_a.next_row(bi)?;
                } else {
                    writer.push_row(opt_b.as_ref().unwrap(), bi)?;
                    opt_b = reader_b.next_row(bi)?;
                }
            }
        }
    }

    writer.finish(bi)
}

// ═══════════════════════════════════════════════════════════════════════════
// CROSS — nested-loop cartesian product
// ═══════════════════════════════════════════════════════════════════════════

/// CrossIter materializes the RIGHT child in memory, then streams the LEFT child.
/// For each left row it pairs it with every right row.
/// Memory: O(|right|) — only the right side lives in RAM.
pub struct CrossIter {
    pub schema: Schema,
    left: RowIter,
    right_child: Option<Box<RowIter>>,
    right_rows: Vec<Row>,
    current_left: Option<Row>,
    right_idx: usize,
    built: bool,
}

impl CrossIter {
    fn next_row<R: BufRead, W: Write>(
        &mut self,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<Option<Row>> {
        // Materialize the right child on the very first call
        if !self.built {
            let mut right_child = self.right_child.take().unwrap();
            while let Some(row) = right_child.next_row(bi)? {
                self.right_rows.push(row);
            }
            self.built = true;
            self.current_left = self.left.next_row(bi)?;
            self.right_idx = 0;
        }

        if self.right_rows.is_empty() {
            return Ok(None);
        }

        loop {
            // Move to next left row when all right rows for current left are done
            if self.right_idx >= self.right_rows.len() {
                self.current_left = self.left.next_row(bi)?;
                self.right_idx = 0;
            }
            if self.current_left.is_none() {
                return Ok(None);
            }
            // Build joined row (left values ++ right values)
            let joined = {
                let left_row = self.current_left.as_ref().unwrap();
                let right_row = &self.right_rows[self.right_idx];
                let mut j = Vec::with_capacity(left_row.len() + right_row.len());
                j.extend_from_slice(left_row);
                j.extend_from_slice(right_row);
                j
            };
            self.right_idx += 1;
            return Ok(Some(joined));
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// BUILD PLAN
// ═══════════════════════════════════════════════════════════════════════════

/// Build a RowIter execution tree from a QueryOp tree.
/// No disk I/O happens here — everything is deferred to next_row().
pub fn build_plan(op: &QueryOp, ctx: &DbContext) -> Result<RowIter> {
    match op {
        QueryOp::Scan(scan_data) => {
            let table = ctx
                .get_table_specs()
                .iter()
                .find(|t| t.name == scan_data.table_id)
                .ok_or_else(|| anyhow!("Table '{}' not found in db_config", scan_data.table_id))?;

            let schema: Schema = table.column_specs.iter().map(|c| c.column_name.clone()).collect();
            let col_types: Vec<DataType> =
                table.column_specs.iter().map(|c| c.data_type.clone()).collect();

            Ok(RowIter::Scan(ScanIter::new(schema, col_types, table.file_id.clone())))
        }

        QueryOp::Filter(filter_data) => {
            let child = build_plan(&filter_data.underlying, ctx)?;
            let schema = child.schema().to_vec();
            Ok(RowIter::Filter(Box::new(FilterIter {
                schema,
                predicates: filter_data.predicates.clone(),
                child,
            })))
        }

        QueryOp::Project(proj_data) => {
            let child = build_plan(&proj_data.underlying, ctx)?;
            let child_schema = child.schema().to_vec();
            let mut out_schema = Vec::new();
            let mut col_indices = Vec::new();
            for (from, to) in &proj_data.column_name_map {
                let idx = child_schema.iter().position(|n| n == from).ok_or_else(|| {
                    anyhow!("Project: column '{}' not in child schema", from)
                })?;
                out_schema.push(to.clone());
                col_indices.push(idx);
            }
            Ok(RowIter::Project(Box::new(ProjectIter {
                out_schema,
                col_indices,
                child,
            })))
        }

        QueryOp::Sort(sort_data) => {
            let child = build_plan(&sort_data.underlying, ctx)?;
            let schema = child.schema().to_vec();
            Ok(RowIter::Sort(Box::new(SortIter {
                schema,
                sort_specs: sort_data.sort_specs.clone(),
                child: Some(Box::new(child)),
                col_types: Vec::new(),
                chunk: Vec::new(),
                chunk_mem: 0,
                runs: Vec::new(),
                run_reader: None,
                phase: 0,
            })))
        }

        QueryOp::Cross(cross_data) => {
            let left = build_plan(&cross_data.left, ctx)?;
            let right = build_plan(&cross_data.right, ctx)?;
            let mut schema = left.schema().to_vec();
            schema.extend_from_slice(right.schema());
            Ok(RowIter::Cross(Box::new(CrossIter {
                schema,
                left,
                right_child: Some(Box::new(right)),
                right_rows: Vec::new(),
                current_left: None,
                right_idx: 0,
                built: false,
            })))
        }
    }
}
