/// optimizer.rs — Query Optimizer
///
/// Role: transform a QueryOp tree into an equivalent but cheaper tree BEFORE
///       any disk I/O takes place.
///
/// Two classical rewrites are applied:
///
///   1. **Selection pushdown** (Filter ↓)
///      Move Filter nodes as close to the leaf Scans as possible.
///      This shrinks the working set before expensive operations like Cross.
///
///   2. **Projection stays up** (Project ↑)
///      Projections are kept at or above their current level.
///      Filters are pushed through them by rewriting column references
///      through the inverse column-name map.
///
/// How Filter is pushed through a Cross join
/// ------------------------------------------
/// A Cross join concatenates left_schema ++ right_schema.
/// A predicate that only references columns from the left side can be pushed
/// to the left child; one that only references the right side goes to the right
/// child.  Predicates that reference both sides (join conditions) remain above
/// the Cross and are later converted to efficient join operators by build_plan.
use common::query::{
    ComparisionValue, CrossData, FilterData, Predicate, ProjectData, QueryOp, ScanData, SortData,
};
use db_config::DbContext;

// ─── Schema helper ────────────────────────────────────────────────────────────

/// Compute the output column names of a QueryOp without executing any disk I/O.
pub fn schema_of(op: &QueryOp, ctx: &DbContext) -> Vec<String> {
    match op {
        QueryOp::Scan(ScanData { table_id }) => ctx
            .get_table_specs()
            .iter()
            .find(|t| &t.name == table_id)
            .map(|t| t.column_specs.iter().map(|c| c.column_name.clone()).collect())
            .unwrap_or_default(),
        QueryOp::Filter(f) => schema_of(&f.underlying, ctx),
        QueryOp::Project(p) => p.column_name_map.iter().map(|(_, to)| to.clone()).collect(),
        QueryOp::Sort(s) => schema_of(&s.underlying, ctx),
        QueryOp::Cross(c) => {
            let mut s = schema_of(&c.left, ctx);
            s.extend(schema_of(&c.right, ctx));
            s
        }
    }
}

// ─── Entry point ──────────────────────────────────────────────────────────────

/// Optimize a QueryOp tree before execution.
///
/// Call this once on the root before passing the tree to `build_plan`.
pub fn optimize(op: QueryOp, ctx: &DbContext) -> QueryOp {
    match op {
        // When we see a Filter, recursively optimize its child first,
        // then try to push the predicates down as far as possible.
        QueryOp::Filter(FilterData { predicates, underlying }) => {
            let child = optimize(*underlying, ctx);
            push_filter_down(predicates, child, ctx)
        }

        // For Cross, just optimize both children independently.
        QueryOp::Cross(CrossData { left, right }) => QueryOp::Cross(CrossData {
            left: Box::new(optimize(*left, ctx)),
            right: Box::new(optimize(*right, ctx)),
        }),

        // For Project, optimize the child but keep the project in place.
        // (Projection stays up — it only removes columns, not rows.)
        QueryOp::Project(ProjectData { column_name_map, underlying }) => {
            QueryOp::Project(ProjectData {
                column_name_map,
                underlying: Box::new(optimize(*underlying, ctx)),
            })
        }

        // For Sort, optimize the child but keep the sort in place.
        QueryOp::Sort(SortData { sort_specs, underlying }) => QueryOp::Sort(SortData {
            sort_specs,
            underlying: Box::new(optimize(*underlying, ctx)),
        }),

        // Scan is a leaf — nothing to push into.
        QueryOp::Scan(_) => op,
    }
}

// ─── Push filter down ─────────────────────────────────────────────────────────

/// Try to push `predicates` through `child`, returning the new subtree.
///
/// Recursively pushes as far down as possible:
///   Cross  → split predicates by which side owns the referenced columns.
///   Filter → merge predicate lists and push the combined set together.
///   Project → rewrite predicate column names through the inverse column map.
///   Scan / Sort / other → wrap a Filter above the child unchanged.
fn push_filter_down(predicates: Vec<Predicate>, child: QueryOp, ctx: &DbContext) -> QueryOp {
    // Nothing to push.
    if predicates.is_empty() {
        return child;
    }

    match child {
        // ── Cross: split predicates onto left / right / above ─────────────────
        QueryOp::Cross(CrossData { left, right }) => {
            let left_cols = schema_of(&left, ctx);
            let right_cols = schema_of(&right, ctx);

            let mut left_preds: Vec<Predicate> = vec![];
            let mut right_preds: Vec<Predicate> = vec![];
            let mut remaining: Vec<Predicate> = vec![];

            for pred in predicates {
                let lhs_in_left = left_cols.contains(&pred.column_name);
                let lhs_in_right = right_cols.contains(&pred.column_name);

                if let ComparisionValue::Column(ref rhs_col) = pred.value {
                    // Column vs column: both refs must be on the same side to push down.
                    let rhs_in_left = left_cols.contains(rhs_col);
                    let rhs_in_right = right_cols.contains(rhs_col);
                    if lhs_in_left && rhs_in_left {
                        left_preds.push(pred);
                    } else if lhs_in_right && rhs_in_right {
                        right_preds.push(pred);
                    } else {
                        // Cross-table join condition — must stay above the Cross.
                        remaining.push(pred);
                    }
                } else {
                    // Scalar predicate: push to whichever side owns the column.
                    if lhs_in_left {
                        left_preds.push(pred);
                    } else if lhs_in_right {
                        right_preds.push(pred);
                    } else {
                        remaining.push(pred);
                    }
                }
            }

            // Wrap each side with its predicates (if any), then recurse.
            let new_left = if left_preds.is_empty() {
                *left
            } else {
                QueryOp::Filter(FilterData { predicates: left_preds, underlying: left })
            };
            let new_right = if right_preds.is_empty() {
                *right
            } else {
                QueryOp::Filter(FilterData { predicates: right_preds, underlying: right })
            };

            let cross = QueryOp::Cross(CrossData {
                left: Box::new(optimize(new_left, ctx)),
                right: Box::new(optimize(new_right, ctx)),
            });

            // Join conditions (if any) stay above the Cross.
            if remaining.is_empty() {
                cross
            } else {
                QueryOp::Filter(FilterData { predicates: remaining, underlying: Box::new(cross) })
            }
        }

        // ── Filter: merge predicate lists, push the union down together ───────
        QueryOp::Filter(FilterData { predicates: child_preds, underlying }) => {
            let mut all = predicates;
            all.extend(child_preds);
            push_filter_down(all, *underlying, ctx)
        }

        // ── Project: rewrite column refs through the inverse map, then push ───
        //
        // Example: Project(a→x, b→y) with Filter(x > 5)
        //   inverse = {x→a, y→b}
        //   rewrite Filter(x > 5) → Filter(a > 5), push below Project.
        //   Project floats back up, filter goes under it.
        QueryOp::Project(ProjectData { column_name_map, underlying }) => {
            // Build inverse map: output_name → original_name.
            let inverse: std::collections::HashMap<String, String> = column_name_map
                .iter()
                .map(|(from, to)| (to.clone(), from.clone()))
                .collect();

            let mut pushable: Vec<Predicate> = vec![];
            let mut remaining: Vec<Predicate> = vec![];

            for pred in predicates {
                // Try to rewrite both LHS column and RHS column (if it's a Column value).
                let new_col = inverse.get(&pred.column_name).cloned();
                let new_val = match &pred.value {
                    ComparisionValue::Column(c) => {
                        // If the RHS column appears in the project output, rewrite it.
                        // If not in the map, it might be a column from a sibling relation
                        // (join condition) — we can still push if lhs is rewriteable and
                        // rhs stays as-is (it will be resolved at the other side).
                        // For safety, only push if rhs is also rewriteable OR it's not in
                        // the project output at all (meaning it wasn't renamed by project).
                        if let Some(rc) = inverse.get(c) {
                            Some(ComparisionValue::Column(rc.clone()))
                        } else {
                            // rhs column not in project output — might be from other side.
                            // We can still push if lhs is rewriteable.
                            Some(ComparisionValue::Column(c.clone()))
                        }
                    }
                    other => Some(other.clone()),
                };

                match (new_col, new_val) {
                    (Some(col), Some(val)) => pushable.push(Predicate {
                        column_name: col,
                        operator: pred.operator,
                        value: val,
                    }),
                    _ => remaining.push(pred),
                }
            }

            // Push rewritten predicates through the Project's child.
            let proj_child = if pushable.is_empty() {
                optimize(*underlying, ctx)
            } else {
                let pushed = push_filter_down(pushable, *underlying, ctx);
                optimize(pushed, ctx)
            };

            let proj = QueryOp::Project(ProjectData {
                column_name_map,
                underlying: Box::new(proj_child),
            });

            // Any predicates that couldn't be rewritten stay above the Project.
            if remaining.is_empty() {
                proj
            } else {
                QueryOp::Filter(FilterData { predicates: remaining, underlying: Box::new(proj) })
            }
        }

        // ── Scan / Sort / anything else: wrap a Filter above it unchanged ─────
        other => QueryOp::Filter(FilterData { predicates, underlying: Box::new(other) }),
    }
}
