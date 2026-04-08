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
use db_config::statistics::ColumnStat;
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
    HashJoin(Box<HashJoinIter>),
    BNLJoin(Box<BNLJoinIter>),
    SMJoin(Box<SMJoinIter>),
    Materialize(Box<MaterializeIter>),
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
            RowIter::HashJoin(h) => &h.schema,
            RowIter::BNLJoin(b) => &b.schema,
            RowIter::SMJoin(s) => &s.schema,
            RowIter::Materialize(m) => &m.schema,
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
            RowIter::HashJoin(h) => h.next_row(bi),
            RowIter::BNLJoin(b) => b.next_row(bi),
            RowIter::SMJoin(s) => s.next_row(bi),
            RowIter::Materialize(m) => m.next_row(bi),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SCAN
// ═══════════════════════════════════════════════════════════════════════════

/// ScanIter streams rows from a disk table file, one block at a time.
/// Memory usage: O(block_size).
///
/// Ordered-scan optimisation: if `early_stop_preds` is non-empty, we check them
/// after every row.  When any predicate fails AND the column is physically ordered
/// (ascending), we know no future row can pass it, so we stop early.
/// This is only set for LT / LTE predicates on columns marked IsPhysicallyOrdered.
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
    /// Predicates that, if they fail, mean we can stop scanning immediately
    /// (because the column is sorted ascending and we've passed the upper bound).
    early_stop_preds: Vec<Predicate>,
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
            early_stop_preds: Vec::new(),
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

        // Early-stop for ordered columns: if an upper-bound predicate fails,
        // the column is sorted ascending so no future row can pass it — stop.
        for pred in &self.early_stop_preds {
            if !evaluate_predicate(&row, pred, &self.schema)? {
                self.done = true;
                return Ok(None);
            }
        }

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
                    if row_passes_all_predicates(&row, &self.predicates, &self.schema)? {
                        return Ok(Some(row));
                    }
                }
            }
        }
    }
}

fn row_passes_all_predicates(row: &Row, predicates: &[Predicate], schema: &[String]) -> Result<bool> {
    for p in predicates {
        if !evaluate_predicate(row, p, schema)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn evaluate_predicate(row: &Row, pred: &Predicate, schema: &[String]) -> Result<bool> {
    let lhs = &row[find_column_index(&pred.column_name, schema)];
    let rhs: Data = match &pred.value {
        ComparisionValue::Column(col) => row[find_column_index(col, schema)].clone(),
        ComparisionValue::I32(v) => Data::Int32(*v),
        ComparisionValue::I64(v) => Data::Int64(*v),
        ComparisionValue::F32(v) => Data::Float32(*v),
        ComparisionValue::F64(v) => Data::Float64(*v),
        ComparisionValue::String(s) => Data::String(s.clone()),
    };
    if std::mem::discriminant(lhs) != std::mem::discriminant(&rhs) {
        return Err(anyhow!(
            "Type mismatch in predicate on column '{}': column value is {} but predicate uses {}",
            pred.column_name,
            data_type_name(lhs),
            data_type_name(&rhs),
        ));
    }
    let cmp = compare_values(lhs, &rhs);
    Ok(match pred.operator {
        ComparisionOperator::EQ => cmp == Some(Ordering::Equal),
        ComparisionOperator::NE => cmp != Some(Ordering::Equal),
        ComparisionOperator::GT => cmp == Some(Ordering::Greater),
        ComparisionOperator::GTE => matches!(cmp, Some(Ordering::Greater) | Some(Ordering::Equal)),
        ComparisionOperator::LT => cmp == Some(Ordering::Less),
        ComparisionOperator::LTE => matches!(cmp, Some(Ordering::Less) | Some(Ordering::Equal)),
    })
}

fn data_type_name(val: &Data) -> &'static str {
    match val {
        Data::Int32(_) => "I32",
        Data::Int64(_) => "I64",
        Data::Float32(_) => "F32",
        Data::Float64(_) => "F64",
        Data::String(_) => "String",
    }
}

/// Compare two Data values. Only same-type comparisons are valid.
fn compare_values(lhs: &Data, rhs: &Data) -> Option<Ordering> {
    lhs.partial_cmp(rhs)
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

/// Fallback sort chunk size used only if memory_limit_mb is unknown (0).
const SORT_CHUNK_MEMORY_FALLBACK: usize = 20 * 1024 * 1024; // 20 MB

/// Compute the sort chunk memory budget from the runtime memory limit.
/// We reserve 25 % of the limit for the sort chunk; the rest is available
/// for LRU cache, join materializations, stack, and code.
fn sort_chunk_budget(memory_limit_mb: u64) -> usize {
    if memory_limit_mb == 0 {
        return SORT_CHUNK_MEMORY_FALLBACK;
    }
    let bytes = (memory_limit_mb as usize) * 1024 * 1024;
    (bytes / 4).max(4 * 1024 * 1024) // at least 4 MB
}

/// Wrap a RowIter in an external SortIter on the given sort specs.
/// Used to build SMJ inputs on-the-fly when hash join would exceed the memory limit.
fn make_sort_iter(child: RowIter, sort_specs: Vec<SortSpec>, memory_limit_mb: u64) -> RowIter {
    let schema = child.schema().to_vec();
    RowIter::Sort(Box::new(SortIter {
        schema,
        sort_specs,
        child: Some(Box::new(child)),
        col_types: Vec::new(),
        chunk: Vec::new(),
        chunk_mem: 0,
        chunk_memory_budget: sort_chunk_budget(memory_limit_mb),
        runs: Vec::new(),
        run_reader: None,
        phase: 0,
    }))
}

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
    /// Memory budget for one in-memory sort chunk (bytes).
    /// Derived from memory_limit_mb at plan-build time.
    chunk_memory_budget: usize,
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

            if self.chunk_mem >= self.chunk_memory_budget {
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
                    let run_a = self.runs[i].clone();
                    let run_b = self.runs[i + 1].clone();
                    let m = merge_two_runs(
                        &run_a,
                        &run_b,
                        &self.col_types,
                        &self.sort_specs,
                        &self.schema,
                        bi,
                    )?;
                    // Return the two source scratch ranges to the free list so
                    // subsequent merges in this round can reuse the blocks.
                    bi.free_scratch_range(run_a.start_block, run_a.num_blocks);
                    bi.free_scratch_range(run_b.start_block, run_b.num_blocks);
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
// HASH JOIN — equi-join using an in-memory hash table on the right side
// ═══════════════════════════════════════════════════════════════════════════

/// Convert a Data value to a string so it can be used as a hash-map key.
/// (Data does not implement Hash because f32/f64 are not Hash.)
fn data_to_key_str(val: &Data) -> String {
    match val {
        Data::Int32(v) => v.to_string(),
        Data::Int64(v) => v.to_string(),
        // Use bit representation so that equal float values produce the same key.
        Data::Float32(v) => v.to_bits().to_string(),
        Data::Float64(v) => v.to_bits().to_string(),
        Data::String(s) => s.clone(),
    }
}

/// HashJoinIter performs an equi-join using a hash table built from the right side.
///
/// Build phase  — materialise the right child into a HashMap keyed by the join
///               column values.
/// Probe phase  — stream left rows one at a time; for each left row look up the
///               hash table and emit all matching (left ++ right) rows.
///
/// Any predicates that are NOT equi-join conditions (e.g. non-equality comparisons)
/// are kept as `post_predicates` and applied as a filter after the hash lookup.
///
/// Memory: O(|right|) for the hash table.
pub struct HashJoinIter {
    pub schema: Schema,
    /// (left_column_index, right_column_index) for each equi-join key.
    join_keys: Vec<(usize, usize)>,
    /// Remaining predicates applied after the hash-table match.
    post_predicates: Vec<Predicate>,
    /// Left child — streamed.
    left: RowIter,
    /// Right child — materialised into `hash_table` on the first call.
    right_child: Option<Box<RowIter>>,
    /// key = [data_to_key_str(row[ri]) for each join key] → list of right rows
    hash_table: std::collections::HashMap<Vec<String>, Vec<Row>>,
    built: bool,
    /// The left row currently being probed.
    current_left: Option<Row>,
    /// Matching right rows for `current_left` fetched from the hash table.
    current_matches: Vec<Row>,
    /// Index into `current_matches`.
    match_idx: usize,
}

impl HashJoinIter {
    fn next_row<R: BufRead, W: Write>(
        &mut self,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<Option<Row>> {
        // ── Build phase: materialise right side into hash table ───────────────
        if !self.built {
            let mut right_child = self.right_child.take().unwrap();
            while let Some(row) = right_child.next_row(bi)? {
                // Build hash key from the right-side join columns.
                let key: Vec<String> = self.join_keys
                    .iter()
                    .map(|(_, ri)| data_to_key_str(&row[*ri]))
                    .collect();
                self.hash_table.entry(key).or_insert_with(Vec::new).push(row);
            }
            self.built = true;
            // Fetch the first left row and its matching right rows.
            self.current_left = self.left.next_row(bi)?;
            self.load_matches();
        }

        // ── Probe phase ───────────────────────────────────────────────────────
        loop {
            if self.current_left.is_none() {
                return Ok(None);
            }

            // Try the next match for the current left row.
            if self.match_idx < self.current_matches.len() {
                let left_row = self.current_left.as_ref().unwrap();
                let right_row = self.current_matches[self.match_idx].clone();
                self.match_idx += 1;

                let mut joined = Vec::with_capacity(left_row.len() + right_row.len());
                joined.extend_from_slice(left_row);
                joined.extend_from_slice(&right_row);

                // Apply any remaining (non-equi) predicates.
                if self.post_predicates.is_empty()
                    || row_passes_all_predicates(&joined, &self.post_predicates, &self.schema)?
                {
                    return Ok(Some(joined));
                }
                continue;
            }

            // All matches exhausted — advance to the next left row.
            self.current_left = self.left.next_row(bi)?;
            self.load_matches();
        }
    }

    /// Pre-load the matching right rows for `current_left` from the hash table.
    fn load_matches(&mut self) {
        self.current_matches.clear();
        self.match_idx = 0;
        if let Some(ref left_row) = self.current_left {
            let key: Vec<String> = self.join_keys
                .iter()
                .map(|(li, _)| data_to_key_str(&left_row[*li]))
                .collect();
            if let Some(matches) = self.hash_table.get(&key) {
                self.current_matches = matches.clone();
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// BLOCK NESTED LOOP JOIN — for non-equi join conditions
// ═══════════════════════════════════════════════════════════════════════════

/// BNLJoinIter performs a nested-loop join with the right side materialised once.
///
/// This is used when there are no equi-join conditions (e.g. `a.price < b.price`),
/// so hash join is not applicable.
///
/// Algorithm:
///   Build — materialise the entire right child in memory (same as CrossIter).
///   Probe — for each left row, iterate over all right rows and emit pairs that
///           satisfy all join predicates.
///
/// The "block" in the name refers to the fact that in a true BNL join the left
/// side is also loaded in blocks to improve cache locality.  In the Volcano
/// iterator model we process one left row at a time, which is equivalent when
/// the right side is already in memory.
///
/// Memory: O(|right|).
pub struct BNLJoinIter {
    pub schema: Schema,
    /// All join predicates (cross-table, non-equi or mixed).
    predicates: Vec<Predicate>,
    /// Left child — streamed.
    left: RowIter,
    /// Right child — materialised into `right_rows` on the first call.
    right_child: Option<Box<RowIter>>,
    right_rows: Vec<Row>,
    built: bool,
    /// Current left row.
    current_left: Option<Row>,
    /// Index into `right_rows` for the current left row.
    right_idx: usize,
}

impl BNLJoinIter {
    fn next_row<R: BufRead, W: Write>(
        &mut self,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<Option<Row>> {
        // ── Build phase: materialise right side ───────────────────────────────
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

        // ── Probe phase ───────────────────────────────────────────────────────
        loop {
            // Advance to the next left row when all right rows are exhausted.
            if self.right_idx >= self.right_rows.len() {
                self.current_left = self.left.next_row(bi)?;
                self.right_idx = 0;
            }
            if self.current_left.is_none() {
                return Ok(None);
            }

            let left_row = self.current_left.as_ref().unwrap();
            let right_row = &self.right_rows[self.right_idx];
            self.right_idx += 1;

            // Build combined row (left ++ right) to evaluate predicates.
            let mut joined = Vec::with_capacity(left_row.len() + right_row.len());
            joined.extend_from_slice(left_row);
            joined.extend_from_slice(right_row);

            if row_passes_all_predicates(&joined, &self.predicates, &self.schema)? {
                return Ok(Some(joined));
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// MATERIALIZE — eager pull-all-then-serve model
// ═══════════════════════════════════════════════════════════════════════════

/// MaterializeIter implements the "materialized" execution model.
///
/// On the **first** call to `next_row` it drains its entire child subtree into
/// an in-memory `Vec<Row>`, then serves subsequent calls from that vector.
///
/// When to use:
///   - When the same subtree output must be replayed multiple times.
///   - When the optimizer decides that eager materialization is cheaper than
///     repeated re-computation (e.g., a small filtered relation used in a join).
///
/// Memory: O(total rows produced by child).
pub struct MaterializeIter {
    pub schema: Schema,
    /// Child operator — consumed on the first call, then set to None.
    child: Option<Box<RowIter>>,
    /// All materialized rows.
    rows: Vec<Row>,
    /// Index of the next row to return.
    cursor: usize,
}

impl MaterializeIter {
    fn next_row<R: BufRead, W: Write>(
        &mut self,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<Option<Row>> {
        // Drain the child on the very first call.
        if let Some(mut child) = self.child.take() {
            while let Some(row) = child.next_row(bi)? {
                self.rows.push(row);
            }
        }
        if self.cursor < self.rows.len() {
            let row = self.rows[self.cursor].clone();
            self.cursor += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SORT-MERGE JOIN — merge two pre-sorted streams on equi-join keys
// ═══════════════════════════════════════════════════════════════════════════

/// Compare a left row and a right row on the join keys.
/// Uses proper typed Data comparison — NOT string conversion — so numeric
/// ordering (e.g. 9 < 100) is always correct.
fn smj_compare(join_keys: &[(usize, usize)], left: &Row, right: &Row) -> Ordering {
    for (li, ri) in join_keys {
        let ord = compare_values(&left[*li], &right[*ri]).unwrap_or(Ordering::Equal);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Return true if two rows from the SAME side share the same join key values.
/// `use_left` selects whether to index by join_keys[i].0 (left) or .1 (right).
fn smj_same_key(join_keys: &[(usize, usize)], a: &Row, b: &Row, use_left: bool) -> bool {
    join_keys.iter().all(|(li, ri)| {
        let idx = if use_left { *li } else { *ri };
        compare_values(&a[idx], &b[idx]) == Some(Ordering::Equal)
    })
}

/// SMJoinIter performs a sort-merge join.
///
/// **Pre-condition**: both `left` and `right` produce rows sorted ascending on
/// their respective join-key columns.  If the inputs are not physically ordered,
/// wrap them in a `SortIter` before creating this operator (handled in build_plan).
///
/// Algorithm (inner equi-join with n:m duplicate support):
///   1. Advance both pointers.
///   2. If left_key < right_key → advance left (no match).
///   3. If left_key > right_key → advance right (no match).
///   4. If keys equal → collect all left rows with this key (left_group) and
///      all right rows with the same key (right_group), then emit the full
///      cross product of the two groups.
///   5. After emitting the group, resume from the rows that follow the group.
///
/// Memory: O(size of one matching key group on each side).
pub struct SMJoinIter {
    pub schema: Schema,
    /// (left_column_index, right_column_index) for each join key.
    join_keys: Vec<(usize, usize)>,
    /// Post-join predicates applied after an equi-join match.
    post_predicates: Vec<Predicate>,
    /// Left input — must be sorted on the left join-key columns.
    left: RowIter,
    /// Right input — must be sorted on the right join-key columns.
    right: RowIter,
    /// Whether both sides have been primed with their first row.
    initialized: bool,
    /// Current peeked row from the left side.
    left_row: Option<Row>,
    /// Current peeked row from the right side.
    right_row: Option<Row>,
    /// Buffered left rows for the current matching key group.
    left_group: Vec<Row>,
    /// Buffered right rows for the current matching key group.
    right_group: Vec<Row>,
    /// Index into left_group for the current emit position.
    left_group_idx: usize,
    /// Index into right_group for the current emit position.
    right_group_idx: usize,
    /// True while we are emitting the cross product of the current groups.
    emitting: bool,
}

impl SMJoinIter {
    fn next_row<R: BufRead, W: Write>(
        &mut self,
        bi: &mut BlockInterface<R, W>,
    ) -> Result<Option<Row>> {
        // Prime both sides on first call.
        if !self.initialized {
            self.left_row = self.left.next_row(bi)?;
            self.right_row = self.right.next_row(bi)?;
            self.initialized = true;
        }

        loop {
            // ── Emit phase: output left_group × right_group ────────────────
            if self.emitting {
                while self.left_group_idx < self.left_group.len() {
                    if self.right_group_idx < self.right_group.len() {
                        let left_row = &self.left_group[self.left_group_idx];
                        let right_row = &self.right_group[self.right_group_idx];

                        let mut joined = Vec::with_capacity(left_row.len() + right_row.len());
                        joined.extend_from_slice(left_row);
                        joined.extend_from_slice(right_row);

                        self.right_group_idx += 1;

                        if !self.post_predicates.is_empty()
                            && !row_passes_all_predicates(
                                &joined,
                                &self.post_predicates,
                                &self.schema,
                            )?
                        {
                            continue;
                        }
                        return Ok(Some(joined));
                    } else {
                        // Exhausted right group for this left row — move to next left.
                        self.right_group_idx = 0;
                        self.left_group_idx += 1;
                    }
                }
                // Finished emitting the entire group.
                self.emitting = false;
                self.left_group.clear();
                self.right_group.clear();
                self.left_group_idx = 0;
                self.right_group_idx = 0;
                // left_row / right_row already point past the group — continue compare.
            }

            // ── Compare phase ──────────────────────────────────────────────
            // Determine the ordering of the current left vs right rows.
            // We use typed Data comparison (smj_compare) so that numeric keys
            // like 9 < 100 are ordered correctly, not lexicographically.
            let ord = match (&self.left_row, &self.right_row) {
                (None, _) | (_, None) => return Ok(None),
                (Some(lr), Some(rr)) => {
                    let jk = &self.join_keys;
                    smj_compare(jk, lr, rr)
                }
            };

            match ord {
                Ordering::Less => {
                    self.left_row = self.left.next_row(bi)?;
                }
                Ordering::Greater => {
                    self.right_row = self.right.next_row(bi)?;
                }
                Ordering::Equal => {
                    // Keys match — collect all left rows with this key into left_group.
                    // We compare against the FIRST row's key; rows after that are
                    // compared to the group's first row (same-side key check).
                    while let Some(lr) = self.left_row.take() {
                        self.left_group.push(lr);
                        self.left_row = self.left.next_row(bi)?;
                        // Stop when the next left row has a different key.
                        let continues = match &self.left_row {
                            Some(next) => {
                                let jk = &self.join_keys;
                                let first = &self.left_group[0];
                                smj_same_key(jk, first, next, true)
                            }
                            None => false,
                        };
                        if !continues {
                            break;
                        }
                    }

                    // Collect all right rows with the same key into right_group.
                    while let Some(rr) = self.right_row.take() {
                        self.right_group.push(rr);
                        self.right_row = self.right.next_row(bi)?;
                        let continues = match &self.right_row {
                            Some(next) => {
                                let jk = &self.join_keys;
                                let first = &self.right_group[0];
                                smj_same_key(jk, first, next, false)
                            }
                            None => false,
                        };
                        if !continues {
                            break;
                        }
                    }

                    self.left_group_idx = 0;
                    self.right_group_idx = 0;
                    self.emitting = true;
                }
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// BUILD PLAN
// ═══════════════════════════════════════════════════════════════════════════

/// Build a RowIter execution tree from a QueryOp tree.
/// No disk I/O happens here — everything is deferred to next_row().
///
/// Join selection logic (applied when Filter directly wraps Cross):
///   1. Both Cross children are Sort nodes on the join key
///      → Sort-Merge Join (avoids materialising either side in full)
///   2. Equi-join condition exists
///      → Hash Join (smaller side used as build table based on CardinalityStat)
///   3. Non-equi join only
///      → Block Nested Loop Join
///
/// Ordered-scan optimisation (Filter directly wraps Scan):
///   If a filter predicate is an upper-bound (LT/LTE) on a column marked
///   IsPhysicallyOrdered, it is converted to an early-stop predicate inside
///   the ScanIter so we avoid reading past the relevant range.
pub fn build_plan(op: &QueryOp, ctx: &DbContext, memory_limit_mb: u64) -> Result<RowIter> {
    match op {
        // ── Scan ─────────────────────────────────────────────────────────────
        QueryOp::Scan(scan_data) => {
            let table = ctx
                .get_table_specs()
                .iter()
                .find(|t| t.name == scan_data.table_id)
                .ok_or_else(|| anyhow!("Table '{}' not found in db_config", scan_data.table_id))?;

            let schema: Schema =
                table.column_specs.iter().map(|c| c.column_name.clone()).collect();
            let col_types: Vec<DataType> =
                table.column_specs.iter().map(|c| c.data_type.clone()).collect();

            Ok(RowIter::Scan(ScanIter::new(schema, col_types, table.file_id.clone())))
        }

        // ── Filter ────────────────────────────────────────────────────────────
        QueryOp::Filter(filter_data) => {
            // ── Optimisation A: Filter(Cross(...)) → join operator ─────────────
            if let QueryOp::Cross(cross_data) = filter_data.underlying.as_ref() {
                return build_join(
                    &filter_data.predicates,
                    &cross_data.left,
                    &cross_data.right,
                    ctx,
                    memory_limit_mb,
                );
            }

            // ── Optimisation B: Filter(Scan(...)) → ordered-scan early stop ────
            if let QueryOp::Scan(scan_data) = filter_data.underlying.as_ref() {
                if let Some(iter) = try_ordered_scan(filter_data, scan_data, ctx)? {
                    return Ok(iter);
                }
            }

            // ── Plain filter ───────────────────────────────────────────────────
            let child = build_plan(&filter_data.underlying, ctx, memory_limit_mb)?;
            let schema = child.schema().to_vec();
            Ok(RowIter::Filter(Box::new(FilterIter {
                schema,
                predicates: filter_data.predicates.clone(),
                child,
            })))
        }

        // ── Project ───────────────────────────────────────────────────────────
        QueryOp::Project(proj_data) => {
            let child = build_plan(&proj_data.underlying, ctx, memory_limit_mb)?;
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

        // ── Sort ──────────────────────────────────────────────────────────────
        QueryOp::Sort(sort_data) => {
            let child = build_plan(&sort_data.underlying, ctx, memory_limit_mb)?;
            let schema = child.schema().to_vec();
            Ok(RowIter::Sort(Box::new(SortIter {
                schema,
                sort_specs: sort_data.sort_specs.clone(),
                child: Some(Box::new(child)),
                col_types: Vec::new(),
                chunk: Vec::new(),
                chunk_mem: 0,
                chunk_memory_budget: sort_chunk_budget(memory_limit_mb),
                runs: Vec::new(),
                run_reader: None,
                phase: 0,
            })))
        }

        // ── Cross (bare cartesian product — no filter above) ─────────────────
        QueryOp::Cross(cross_data) => {
            let left = build_plan(&cross_data.left, ctx, memory_limit_mb)?;
            let right = build_plan(&cross_data.right, ctx, memory_limit_mb)?;
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

// ─── build_join: choose the right join algorithm ──────────────────────────────

/// Decide which join operator to build for a Filter(Cross(left, right)) pattern.
///
/// Decision tree:
///   1. Both children are Sort nodes on the equi-join key → Sort-Merge Join.
///   2. Equi-join predicates exist → Hash Join.
///      (We optionally swap left/right so the smaller table is the build side.)
///   3. No equi-join → Block Nested Loop Join.
fn build_join(
    predicates: &[Predicate],
    left_op: &QueryOp,
    right_op: &QueryOp,
    ctx: &DbContext,
    memory_limit_mb: u64,
) -> Result<RowIter> {
    // Classify predicates into equi-join keys and remaining post-predicates.
    // We need the schemas to determine which side each column belongs to.
    use crate::optimizer::schema_of;
    let left_schema_cols = schema_of(left_op, ctx);
    let right_schema_cols = schema_of(right_op, ctx);

    let mut equi_keys: Vec<(usize, usize)> = vec![];
    let mut post_preds: Vec<Predicate> = vec![];

    for pred in predicates {
        if matches!(pred.operator, ComparisionOperator::EQ) {
            if let ComparisionValue::Column(ref rhs_col) = pred.value {
                // Try lhs∈left, rhs∈right
                let li = left_schema_cols.iter().position(|c| c == &pred.column_name);
                let ri = right_schema_cols.iter().position(|c| c == rhs_col);
                if let (Some(l), Some(r)) = (li, ri) {
                    equi_keys.push((l, r));
                    continue;
                }
                // Try reversed orientation: lhs∈right, rhs∈left
                let li2 = left_schema_cols.iter().position(|c| c == rhs_col);
                let ri2 = right_schema_cols.iter().position(|c| c == &pred.column_name);
                if let (Some(l), Some(r)) = (li2, ri2) {
                    equi_keys.push((l, r));
                    continue;
                }
            }
        }
        post_preds.push(pred.clone());
    }

    if equi_keys.is_empty() {
        // ── Block Nested Loop Join ────────────────────────────────────────────
        let left_plan = build_plan(left_op, ctx, memory_limit_mb)?;
        let right_plan = build_plan(right_op, ctx, memory_limit_mb)?;
        let left_schema = left_plan.schema().to_vec();
        let right_schema = right_plan.schema().to_vec();
        let mut combined = left_schema;
        combined.extend_from_slice(&right_schema);
        return Ok(RowIter::BNLJoin(Box::new(BNLJoinIter {
            schema: combined,
            predicates: predicates.to_vec(),
            left: left_plan,
            right_child: Some(Box::new(right_plan)),
            right_rows: Vec::new(),
            built: false,
            current_left: None,
            right_idx: 0,
        })));
    }

    // ── Equi-join: decide between Sort-Merge Join and Hash Join ──────────────

    // Check if both children are Sort nodes on the equi-join columns.
    let left_sorted_on_key = is_sorted_on_join_key(left_op, &equi_keys, true, &left_schema_cols);
    let right_sorted_on_key =
        is_sorted_on_join_key(right_op, &equi_keys, false, &right_schema_cols);

    if left_sorted_on_key && right_sorted_on_key {
        // ── Sort-Merge Join ───────────────────────────────────────────────────
        let left_plan = build_plan(left_op, ctx, memory_limit_mb)?;
        let right_plan = build_plan(right_op, ctx, memory_limit_mb)?;
        let left_schema = left_plan.schema().to_vec();
        let right_schema = right_plan.schema().to_vec();
        let mut combined = left_schema;
        combined.extend_from_slice(&right_schema);
        return Ok(RowIter::SMJoin(Box::new(SMJoinIter {
            schema: combined,
            join_keys: equi_keys,
            post_predicates: post_preds,
            left: left_plan,
            right: right_plan,
            initialized: false,
            left_row: None,
            right_row: None,
            left_group: Vec::new(),
            right_group: Vec::new(),
            left_group_idx: 0,
            right_group_idx: 0,
            emitting: false,
        })));
    }

    // ── Hash Join ─────────────────────────────────────────────────────────────
    // Use CardinalityStat to pick the smaller side as the build table.
    // Building on the smaller side reduces hash-table memory and build time.
    let left_card = estimated_cardinality(left_op, ctx);
    let right_card = estimated_cardinality(right_op, ctx);

    // If left is smaller, swap so left becomes the probe side and right the build.
    // HashJoinIter always builds from the right child.
    let (left_op, right_op, equi_keys) = if left_card < right_card {
        // Swap: left (smaller) becomes build (right), right becomes probe (left).
        // Must also flip the join key indices.
        let flipped: Vec<(usize, usize)> = equi_keys.into_iter().map(|(l, r)| (r, l)).collect();
        (right_op, left_op, flipped)
    } else {
        (left_op, right_op, equi_keys)
    };

    // ── Memory-safety check: fall back to SMJ if build side is too large ──────
    // Hash join materialises the entire right (build) side in RAM.  If the
    // estimated row count × a conservative per-row size exceeds the memory
    // budget, wrap both inputs with an external Sort and use Sort-Merge Join
    // instead — it only holds O(block_size) bytes per side at a time.
    // When cardinality is unknown (u64::MAX), saturating_mul keeps it at
    // u64::MAX, which always exceeds the budget → safe conservative fallback.
    let build_card = estimated_cardinality(right_op, ctx);
    let memory_limit_bytes = (memory_limit_mb as u64).saturating_mul(1024 * 1024);
    if build_card.saturating_mul(512) > memory_limit_bytes {
        let left_key_cols: Vec<String> = {
            let s = schema_of(left_op, ctx);
            equi_keys.iter().map(|(li, _)| s[*li].clone()).collect()
        };
        let right_key_cols: Vec<String> = {
            let s = schema_of(right_op, ctx);
            equi_keys.iter().map(|(_, ri)| s[*ri].clone()).collect()
        };

        let left_plan = build_plan(left_op, ctx, memory_limit_mb)?;
        let right_plan = build_plan(right_op, ctx, memory_limit_mb)?;
        let left_schema = left_plan.schema().to_vec();
        let right_schema = right_plan.schema().to_vec();

        let left_sort_specs: Vec<SortSpec> = left_key_cols
            .into_iter()
            .map(|col| SortSpec { column_name: col, ascending: true })
            .collect();
        let right_sort_specs: Vec<SortSpec> = right_key_cols
            .into_iter()
            .map(|col| SortSpec { column_name: col, ascending: true })
            .collect();

        let left_sorted = make_sort_iter(left_plan, left_sort_specs, memory_limit_mb);
        let right_sorted = make_sort_iter(right_plan, right_sort_specs, memory_limit_mb);

        let mut combined = left_schema;
        combined.extend_from_slice(&right_schema);
        return Ok(RowIter::SMJoin(Box::new(SMJoinIter {
            schema: combined,
            join_keys: equi_keys,
            post_predicates: post_preds,
            left: left_sorted,
            right: right_sorted,
            initialized: false,
            left_row: None,
            right_row: None,
            left_group: Vec::new(),
            right_group: Vec::new(),
            left_group_idx: 0,
            right_group_idx: 0,
            emitting: false,
        })));
    }

    let left_plan = build_plan(left_op, ctx, memory_limit_mb)?;
    let right_plan = build_plan(right_op, ctx, memory_limit_mb)?;
    let left_schema = left_plan.schema().to_vec();
    let right_schema = right_plan.schema().to_vec();
    let mut combined = left_schema;
    combined.extend_from_slice(&right_schema);

    Ok(RowIter::HashJoin(Box::new(HashJoinIter {
        schema: combined,
        join_keys: equi_keys,
        post_predicates: post_preds,
        left: left_plan,
        right_child: Some(Box::new(right_plan)),
        hash_table: std::collections::HashMap::new(),
        built: false,
        current_left: None,
        current_matches: Vec::new(),
        match_idx: 0,
    })))
}

/// Return true if `op` is (or contains) a Sort node whose first sort key
/// matches one of the equi-join key columns.
fn is_sorted_on_join_key(
    op: &QueryOp,
    equi_keys: &[(usize, usize)],
    use_left_idx: bool,
    schema_cols: &[String],
) -> bool {
    if let QueryOp::Sort(sort_data) = op {
        if let Some(first_spec) = sort_data.sort_specs.first() {
            // The first sort key must be one of the join key columns.
            return equi_keys.iter().any(|(li, ri)| {
                let idx = if use_left_idx { *li } else { *ri };
                schema_cols.get(idx).map_or(false, |col| col == &first_spec.column_name)
            });
        }
    }
    false
}

/// Estimate the number of rows in a QueryOp subtree using CardinalityStat.
/// Returns u64::MAX if no stat is available (treat as large / unknown).
fn estimated_cardinality(op: &QueryOp, ctx: &DbContext) -> u64 {
    // Walk down to the underlying Scan to find table stats.
    match op {
        QueryOp::Scan(scan) => {
            if let Some(table) = ctx.get_table_specs().iter().find(|t| t.name == scan.table_id) {
                for col_spec in &table.column_specs {
                    if let Some(stats) = &col_spec.stats {
                        for stat in stats {
                            if let ColumnStat::CardinalityStat(c) = stat {
                                return c.0;
                            }
                        }
                    }
                }
            }
            u64::MAX
        }
        QueryOp::Filter(f) => estimated_cardinality(&f.underlying, ctx),
        QueryOp::Sort(s) => estimated_cardinality(&s.underlying, ctx),
        QueryOp::Project(p) => estimated_cardinality(&p.underlying, ctx),
        QueryOp::Cross(_) => u64::MAX,
    }
}

// ─── try_ordered_scan: Filter(Scan) with early-stop optimisation ──────────────

/// Try to build a ScanIter with early-stop predicates when the scan column
/// is physically ordered.
///
/// Returns Some(RowIter) if the optimisation applies, None otherwise (caller
/// falls through to the plain filter path).
fn try_ordered_scan(
    filter_data: &common::query::FilterData,
    scan_data: &common::query::ScanData,
    ctx: &DbContext,
) -> Result<Option<RowIter>> {
    let table = match ctx
        .get_table_specs()
        .iter()
        .find(|t| t.name == scan_data.table_id)
    {
        Some(t) => t,
        None => return Ok(None),
    };

    let schema: Schema = table.column_specs.iter().map(|c| c.column_name.clone()).collect();
    let col_types: Vec<DataType> =
        table.column_specs.iter().map(|c| c.data_type.clone()).collect();

    // Identify which columns are physically ordered.
    let ordered_cols: std::collections::HashSet<String> = table
        .column_specs
        .iter()
        .filter(|cs| {
            cs.stats.as_ref().map_or(false, |stats| {
                stats.iter().any(|s| matches!(s, ColumnStat::IsPhysicallyOrdered))
            })
        })
        .map(|cs| cs.column_name.clone())
        .collect();

    if ordered_cols.is_empty() {
        return Ok(None); // no ordered column → no early-stop possible
    }

    // Partition filter predicates:
    //   early_stop_preds — LT/LTE on a physically-ordered column (we stop when they fail)
    //   remaining_preds  — everything else (applied in a FilterIter above the scan)
    let mut early_stop_preds: Vec<Predicate> = Vec::new();
    let mut remaining_preds: Vec<Predicate> = Vec::new();

    for pred in &filter_data.predicates {
        let is_upper_bound = matches!(
            pred.operator,
            ComparisionOperator::LT | ComparisionOperator::LTE
        );
        // Only scalar comparisons can be used for early stop
        // (Column-vs-column comparisons need both values to evaluate).
        let is_scalar = !matches!(pred.value, ComparisionValue::Column(_));

        if is_upper_bound && is_scalar && ordered_cols.contains(&pred.column_name) {
            early_stop_preds.push(pred.clone());
        } else {
            remaining_preds.push(pred.clone());
        }
    }

    if early_stop_preds.is_empty() {
        return Ok(None); // no early-stop predicate found
    }

    // Build a ScanIter with the early-stop predicates embedded.
    let mut scan_iter = ScanIter::new(schema.clone(), col_types, table.file_id.clone());
    scan_iter.early_stop_preds = early_stop_preds;

    let scan = RowIter::Scan(scan_iter);

    // Wrap remaining predicates in a FilterIter above the scan.
    if remaining_preds.is_empty() {
        Ok(Some(scan))
    } else {
        Ok(Some(RowIter::Filter(Box::new(FilterIter {
            schema,
            predicates: remaining_preds,
            child: scan,
        }))))
    }
}
