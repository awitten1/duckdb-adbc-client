//! Direct physical plans for the two-range ADBC sort benchmark.

use super::{fail, fnv1a, spawn_reader, with_clickhouse_settings, ChannelPartition, Options, Res};
use adbc_driver_manager::ManagedDriver;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, Partitioning, PhysicalSortExpr};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::streaming::StreamingTableExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use std::sync::{Arc, Mutex};
use std::time::Instant;

const SCALE: u32 = 1_000_000_000;
const ASC_NULLS_LAST: SortOptions = SortOptions {
    descending: false,
    nulls_first: false,
};

pub(super) fn parse_alpha(text: &str) -> u32 {
    let alpha = text.parse::<f64>().unwrap_or_else(|_| fail("bad --alpha"));
    if !(0.0..=1.0).contains(&alpha) {
        fail("--alpha values must be between 0 and 1");
    }
    (alpha * f64::from(SCALE)).round() as u32
}

fn branches(o: &Options, lo: i64, hi: i64, alpha: u32) -> Vec<(String, bool)> {
    let select = format!("SELECT * FROM {}.{}", o.database, o.table);
    let order = format!("ORDER BY {} ASC NULLS LAST", o.order_by);
    if alpha == SCALE {
        return vec![(format!("{select} {order}"), true)];
    }

    let width = i128::from(hi) - i128::from(lo) + 1;
    let remote_width = width * i128::from(alpha) / i128::from(SCALE);
    if remote_width == 0 {
        return vec![(select, false)];
    }

    let boundary = i128::from(hi) + 1 - remote_width;
    let key = &o.key;
    vec![
        (
            format!("{select} WHERE {key} IS NULL OR {key} < {boundary}"),
            false,
        ),
        (format!("{select} WHERE {key} >= {boundary} {order}"), true),
    ]
}

async fn execute(
    o: &Options,
    ctx: &SessionContext,
    driver: &ManagedDriver,
    uri: &str,
    branches: Vec<(String, bool)>,
    show_plan: bool,
) -> Res<(u64, u128)> {
    let readers: Vec<_> = branches
        .iter()
        .map(|(sql, _)| {
            spawn_reader(
                driver,
                uri,
                with_clickhouse_settings(o, branches.len(), sql.clone()),
            )
        })
        .collect();
    let mut handles = Vec::with_capacity(readers.len());
    let mut inputs = Vec::with_capacity(readers.len());
    for ((schema_rx, receiver, handle), (_, remote)) in readers.into_iter().zip(branches) {
        handles.push(handle);
        inputs.push((schema_rx, receiver, remote));
    }
    let result = async {
        let mut children: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        let mut merge_order = None;
        let mut key_index = None;
        for (schema_rx, receiver, remote) in inputs {
            let schema = schema_rx
                .await
                .map_err(|_| "ADBC reader returned no schema")??;
            let index = schema.index_of(&o.order_by).map_err(|e| e.to_string())?;
            let ordering = LexOrdering::new([PhysicalSortExpr::new(
                Arc::new(Column::new(&o.order_by, index)),
                ASC_NULLS_LAST,
            )])
            .unwrap();
            let partition = Arc::new(ChannelPartition {
                schema: schema.clone(),
                receiver: Mutex::new(Some(receiver)),
            });
            let scan = StreamingTableExec::try_new(
                schema,
                vec![partition],
                None,
                remote.then(|| ordering.clone()),
                false,
                None,
            )
            .map_err(|e| e.to_string())?;
            let mut child: Arc<dyn ExecutionPlan> = Arc::new(scan);
            if !remote {
                if o.threads > 1 {
                    child = Arc::new(
                        RepartitionExec::try_new(child, Partitioning::RoundRobinBatch(o.threads))
                            .map_err(|e| e.to_string())?,
                    );
                }
                child = Arc::new(
                    SortExec::new(ordering.clone(), child).with_preserve_partitioning(true),
                );
                if o.threads > 1 {
                    child = Arc::new(SortPreservingMergeExec::new(ordering.clone(), child));
                }
            }
            children.push(child);
            merge_order = Some(ordering);
            key_index = Some(index);
        }
        let merge = children.len() > 1;
        let mut plan = UnionExec::try_new(children).map_err(|e| e.to_string())?;
        if merge {
            plan = Arc::new(SortPreservingMergeExec::new(merge_order.unwrap(), plan));
        }
        if show_plan {
            eprintln!("{}", displayable(plan.as_ref()).indent(true));
        }
        let mut stream = plan.execute(0, ctx.task_ctx()).map_err(|e| e.to_string())?;
        let key_index = key_index.unwrap();
        let schema = stream.schema();
        let mut converters = if o.checksum {
            let key = SortField::new_with_options(
                schema.field(key_index).data_type().clone(),
                ASC_NULLS_LAST,
            );
            let all = schema
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect();
            Some((
                RowConverter::new(vec![key]).map_err(|e| e.to_string())?,
                RowConverter::new(all).map_err(|e| e.to_string())?,
            ))
        } else {
            None
        };
        let mut rows = 0u64;
        let mut hash = 0u128;
        let mut previous_key: Option<Vec<u8>> = None;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| e.to_string())?;
            rows += batch.num_rows() as u64;
            if let Some((key_converter, row_converter)) = converters.as_mut() {
                let keys = key_converter
                    .convert_columns(&[batch.column(key_index).clone()])
                    .map_err(|e| e.to_string())?;
                let mut last = previous_key.as_deref();
                for key in keys.iter() {
                    if last.is_some_and(|prev| prev > key.data()) {
                        return Err(format!("output is not sorted by {}", o.order_by));
                    }
                    last = Some(key.data());
                }
                previous_key = last.map(<[u8]>::to_vec);
                let encoded = row_converter
                    .convert_columns(batch.columns())
                    .map_err(|e| e.to_string())?;
                hash = encoded.iter().fold(hash, |acc, row| {
                    acc.wrapping_add(u128::from(fnv1a(row.as_ref())))
                });
            }
        }
        Ok((rows, hash))
    }
    .await;
    tokio::task::spawn_blocking(move || {
        for handle in handles {
            handle.join().map_err(|_| "ADBC reader panicked")?;
        }
        Ok::<(), &str>(())
    })
    .await
    .map_err(|e| e.to_string())??;
    result
}

pub(super) async fn sweep(
    o: &Options,
    ctx: &SessionContext,
    driver: &ManagedDriver,
    uri: &str,
    (lo, hi, expected_rows): (i64, i64, i64),
    alphas: &[u32],
) -> Res<()> {
    if o.repeats == 0 {
        return Err("--repeats must be positive".into());
    }
    println!("sort_key\talpha\trepeat\twall_s\trows\tchecksum");
    let mut reference_hash = None;
    for repeat in 1..=o.repeats {
        let order = if repeat % 2 == 1 {
            alphas.iter().copied().collect::<Vec<_>>()
        } else {
            alphas.iter().rev().copied().collect()
        };
        for alpha in order {
            let label = f64::from(alpha) / f64::from(SCALE);
            let started = Instant::now();
            let (rows, hash) =
                execute(o, ctx, driver, uri, branches(o, lo, hi, alpha), repeat == 1)
                    .await
                    .map_err(|e| format!("alpha {label}: {e}"))?;
            let seconds = started.elapsed().as_secs_f64();
            if rows != expected_rows as u64 {
                return Err(format!(
                    "alpha {label}: expected {expected_rows} rows, got {rows}"
                ));
            }
            if o.checksum && *reference_hash.get_or_insert(hash) != hash {
                return Err(format!(
                    "alpha {label}: row checksum differs from the first run"
                ));
            }
            println!(
                "{}\t{label}\t{repeat}\t{seconds:.6}\t{rows}\t{hash}",
                o.order_by
            );
        }
    }
    Ok(())
}
