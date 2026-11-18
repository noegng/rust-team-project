//! End-to-end network benchmark: 3 validator nodes, each in its own Tokio task,
//! and a 4th task sending many TXs to node 0 over HTTP.
//!
//! Run: `cargo bench -p node --features testnet`
//! Quick check: `cargo bench -p node --features testnet -- --test`
//!
//! Criterion measures the wall time; the network statistics it cannot express
//! (error rate, sync resets, propagation...) are printed after the run and
//! written to `target/criterion/network_stats.json`.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{LazyLock, Mutex},
    time::{Duration, Instant},
};

use criterion::{BenchmarkId, Criterion, SamplingMode, Throughput};
use node::testnet::{Load, LoadReport, Testnet, send_load, sender_key};
use serde::Serialize;
use tokio::runtime::Runtime;

const VALIDATORS: u8 = 3;
const BLOCK_TIME: Duration = Duration::from_millis(50);
const BATCH_SIZE: u64 = 100;
const SYNC_TIMEOUT: Duration = Duration::from_secs(60);
const LOAD_SIZES: [u64; 3] = [100, 1_000, 5_000];
const BAD_BLOCKS: u64 = 5;
const CHAIN_TXS: u64 = 1_000;

/// One measured iteration.
#[derive(Default)]
struct Run {
    sync: Duration,
    /// Time from the end of the load to a consistent network.
    lag: Duration,
    load: LoadReport,
    bad_blocks: u64,
    sync_resets: u64,
    propagation_ms: Vec<u64>,
    finality_ms: Vec<u64>,
}

static RUNS: LazyLock<Mutex<BTreeMap<String, Vec<Run>>>> = LazyLock::new(Default::default);

fn record(id: &str, run: Run) {
    RUNS.lock().unwrap().entry(id.into()).or_default().push(run);
}

async fn network_totals(network: &Testnet, run: &mut Run) {
    for node in 0..network.len() {
        let stats = network.stats(node).await;
        run.sync_resets += stats.sync_resets;
        run.propagation_ms.extend(stats.avg_propagation_ms);
        run.finality_ms.extend(stats.avg_finality_ms);
    }
}

/// Load from a 4th task until the 3 nodes agree on every accepted TX.
async fn load_until_synced(count: u64, rogue_blocks: u64) -> Run {
    let network = Testnet::spawn(VALIDATORS, BLOCK_TIME)
        .await
        .expect("testnet starts");
    let url = network.http_url(0);
    let started = Instant::now();
    let loader = tokio::spawn(async move {
        let load = Load {
            sender: sender_key(count),
            count,
            batch_size: BATCH_SIZE,
            forged_every: None,
        };
        send_load(&url, load).await
    });
    for _ in 0..rogue_blocks {
        network
            .send_bad_block(1)
            .await
            .expect("bad block is rejected");
    }
    let load = loader.await.expect("load task");
    let loaded = started.elapsed();
    network
        .wait_until_synced(load.accepted, SYNC_TIMEOUT)
        .await
        .expect("network syncs");
    let sync = started.elapsed();
    let mut run = Run {
        sync,
        lag: sync - loaded,
        load,
        bad_blocks: rogue_blocks,
        ..Run::default()
    };
    network_totals(&network, &mut run).await;
    run
}

fn bench_sync(c: &mut Criterion, runtime: &Runtime) {
    let mut group = c.benchmark_group("sync");
    group
        .sampling_mode(SamplingMode::Flat)
        .sample_size(10)
        .measurement_time(Duration::from_secs(20));
    for count in LOAD_SIZES {
        let id = format!("sync/load_to_sync/{count}");
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::new("load_to_sync", count), &count, |b, &n| {
            b.to_async(runtime).iter_custom(|iters| {
                let id = id.clone();
                async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let run = load_until_synced(n, 0).await;
                        total += run.sync;
                        record(&id, run);
                    }
                    total
                }
            });
        });
    }
    group.finish();
}

fn bench_bad_sync(c: &mut Criterion, runtime: &Runtime) {
    let mut group = c.benchmark_group("bad_sync");
    group
        .sampling_mode(SamplingMode::Flat)
        .sample_size(10)
        .measurement_time(Duration::from_secs(10));

    // Detection: time for a node to drop a peer that sent an invalid block.
    let id = "bad_sync/reset_on_bad_block";
    group.bench_function("reset_on_bad_block", |b| {
        b.to_async(runtime).iter_custom(|iters| async move {
            let network = Testnet::spawn(VALIDATORS, BLOCK_TIME)
                .await
                .expect("testnet starts");
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let before = network.stats(1).await.sync_resets;
                let reset = network.send_bad_block(1).await.expect("bad block rejected");
                total += reset;
                record(
                    id,
                    Run {
                        sync: reset,
                        bad_blocks: 1,
                        sync_resets: network.stats(1).await.sync_resets - before,
                        ..Run::default()
                    },
                );
            }
            total
        });
    });

    // Recovery: the same load as `sync`, while a rogue peer attacks node 1.
    let id = format!("bad_sync/load_with_bad_blocks/{}", LOAD_SIZES[1]);
    group.throughput(Throughput::Elements(LOAD_SIZES[1]));
    group.bench_function(
        BenchmarkId::new("load_with_bad_blocks", LOAD_SIZES[1]),
        |b| {
            b.to_async(runtime).iter_custom(|iters| {
                let id = id.clone();
                async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let run = load_until_synced(LOAD_SIZES[1], BAD_BLOCKS).await;
                        total += run.sync;
                        record(&id, run);
                    }
                    total
                }
            });
        },
    );
    group.finish();
}

fn bench_catch_up(c: &mut Criterion, runtime: &Runtime) {
    let mut group = c.benchmark_group("catch_up");
    group
        .sampling_mode(SamplingMode::Flat)
        .sample_size(10)
        .measurement_time(Duration::from_secs(20));
    let id = format!("catch_up/late_node/{CHAIN_TXS}");
    group.throughput(Throughput::Elements(CHAIN_TXS));
    group.bench_function(BenchmarkId::new("late_node", CHAIN_TXS), |b| {
        b.to_async(runtime).iter_custom(|iters| {
            let id = id.clone();
            async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    // Setup, not measured: a chain holding CHAIN_TXS TXs.
                    let mut network = Testnet::spawn(VALIDATORS, BLOCK_TIME)
                        .await
                        .expect("testnet starts");
                    let load = Load {
                        sender: sender_key(CHAIN_TXS),
                        count: CHAIN_TXS,
                        batch_size: BATCH_SIZE,
                        forged_every: None,
                    };
                    let load = send_load(&network.http_url(0), load).await;
                    network
                        .wait_until_synced(load.accepted, SYNC_TIMEOUT)
                        .await
                        .expect("network syncs");
                    let height = network.stats(2).await.chain_len;

                    let started = Instant::now();
                    let follower = network.spawn_follower(2).await.expect("follower starts");
                    network
                        .wait_for_height(follower, height, SYNC_TIMEOUT)
                        .await
                        .expect("follower catches up");
                    let sync = started.elapsed();
                    total += sync;
                    let mut run = Run {
                        sync,
                        load,
                        ..Run::default()
                    };
                    network_totals(&network, &mut run).await;
                    record(&id, run);
                }
                total
            }
        });
    });
    group.finish();
}

#[derive(Serialize)]
struct Summary {
    runs: usize,
    avg_sync_ms: f64,
    p50_sync_ms: f64,
    p95_sync_ms: f64,
    max_sync_ms: f64,
    avg_lag_ms: f64,
    txs_submitted: u64,
    txs_accepted: u64,
    error_rate: f64,
    failed_requests_txs: u64,
    sync_resets: u64,
    bad_blocks: u64,
    resets_per_bad_block: Option<f64>,
    /// Resets that no rogue block explains (honest nodes out of sync).
    unexpected_resets: u64,
    avg_propagation_ms: Option<f64>,
    avg_finality_ms: Option<f64>,
    tps_to_sync: Option<f64>,
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn mean(values: &[u64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<u64>() as f64 / values.len() as f64)
}

fn summarize(runs: &[Run]) -> Summary {
    let mut syncs: Vec<_> = runs.iter().map(|run| millis(run.sync)).collect();
    syncs.sort_by(f64::total_cmp);
    let percentile = |p: f64| syncs[((syncs.len() - 1) as f64 * p).round() as usize];
    let count = runs.len() as f64;
    let sum = |f: fn(&Run) -> u64| runs.iter().map(f).sum::<u64>();
    let submitted = sum(|run| run.load.submitted);
    let accepted = sum(|run| run.load.accepted);
    let errors = sum(|run| run.load.rejected + run.load.failed);
    let sync_resets = sum(|run| run.sync_resets);
    let bad_blocks = sum(|run| run.bad_blocks);
    let all = |f: fn(&Run) -> &Vec<u64>| runs.iter().flat_map(f).copied().collect::<Vec<_>>();
    let total_sync: f64 = runs.iter().map(|run| run.sync.as_secs_f64()).sum();
    Summary {
        runs: runs.len(),
        avg_sync_ms: syncs.iter().sum::<f64>() / count,
        p50_sync_ms: percentile(0.5),
        p95_sync_ms: percentile(0.95),
        max_sync_ms: syncs[syncs.len() - 1],
        avg_lag_ms: runs.iter().map(|run| millis(run.lag)).sum::<f64>() / count,
        txs_submitted: submitted,
        txs_accepted: accepted,
        error_rate: if submitted == 0 {
            0.0
        } else {
            errors as f64 / submitted as f64
        },
        failed_requests_txs: sum(|run| run.load.failed),
        sync_resets,
        bad_blocks,
        resets_per_bad_block: (bad_blocks > 0).then(|| sync_resets as f64 / bad_blocks as f64),
        unexpected_resets: sync_resets.saturating_sub(bad_blocks),
        avg_propagation_ms: mean(&all(|run| &run.propagation_ms)),
        avg_finality_ms: mean(&all(|run| &run.finality_ms)),
        tps_to_sync: (accepted > 0 && total_sync > 0.0).then(|| accepted as f64 / total_sync),
    }
}

fn print_report() {
    let runs = RUNS.lock().unwrap();
    let summaries: BTreeMap<_, _> = runs
        .iter()
        .filter(|(_, runs)| !runs.is_empty())
        .map(|(id, runs)| (id.clone(), summarize(runs)))
        .collect();
    let opt = |value: Option<f64>| value.map_or("-".into(), |v| format!("{v:.2}"));
    println!("\n=== network statistics ({VALIDATORS} validators, block time {BLOCK_TIME:?}) ===");
    for (id, s) in &summaries {
        println!("{id}");
        println!(
            "  runs {:>4} | sync avg {:.1} ms  p50 {:.1}  p95 {:.1}  max {:.1} | lag after load {:.1} ms",
            s.runs, s.avg_sync_ms, s.p50_sync_ms, s.p95_sync_ms, s.max_sync_ms, s.avg_lag_ms
        );
        println!(
            "  txs {}/{} accepted | error rate {:.4} | failed {} | resets {} (bad blocks {}, per bad block {}, unexpected {})",
            s.txs_accepted,
            s.txs_submitted,
            s.error_rate,
            s.failed_requests_txs,
            s.sync_resets,
            s.bad_blocks,
            opt(s.resets_per_bad_block),
            s.unexpected_resets
        );
        println!(
            "  propagation {} ms | finality {} ms | tps to sync {}",
            opt(s.avg_propagation_ms),
            opt(s.avg_finality_ms),
            opt(s.tps_to_sync)
        );
    }
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"));
    let path = target.join("criterion/network_stats.json");
    let written = std::fs::create_dir_all(path.parent().expect("file has a parent"))
        .and_then(|()| {
            std::fs::write(&path, serde_json::to_vec_pretty(&summaries).expect("serializable"))
        });
    match written {
        Ok(()) => println!("statistics written to {}", path.display()),
        Err(error) => eprintln!("could not write {}: {error}", path.display()),
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let mut criterion = Criterion::default().configure_from_args();
    bench_sync(&mut criterion, &runtime);
    bench_bad_sync(&mut criterion, &runtime);
    bench_catch_up(&mut criterion, &runtime);
    criterion.final_summary();
    print_report();
}
