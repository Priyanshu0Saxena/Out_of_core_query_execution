// recursive query tree execution
// Execute each query operation (QueryOp) recursively - Scan, Filter, Project, Cross, Sort

use anyhow::Result;
use common::query::QueryOp;
use common::Data;
use db_config::DbContext;
use crate::block_interface::BlockInterface;

// A Row is just a list of (column_name, value) pairs
pub type Row = Vec<(String, Data)>;

/// Main entry point — recursively executes the query tree
/// Returns all result rows
pub fn execute(
    op: &QueryOp,
    ctx: &DbContext,
    block_interface: &mut BlockInterface,
) -> Result<Vec<Row>> {
    match op {
        QueryOp::Scan(scan_data) => exec_scan(scan_data, ctx, block_interface),
        QueryOp::Filter(filter_data) => exec_filter(filter_data, ctx, block_interface),
        QueryOp::Project(project_data) => exec_project(project_data, ctx, block_interface),
        QueryOp::Sort(sort_data) => exec_sort(sort_data, ctx, block_interface),
        QueryOp::Cross(cross_data) => exec_cross(cross_data, ctx, block_interface),
    }
}

fn exec_scan(...) -> Result<Vec<Row>> { todo!() }
fn exec_filter(...) -> Result<Vec<Row>> { todo!() }
fn exec_project(...) -> Result<Vec<Row>> { todo!() }
fn exec_sort(...) -> Result<Vec<Row>> { todo!() }
fn exec_cross(...) -> Result<Vec<Row>> { todo!() }