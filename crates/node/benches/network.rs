//! End-to-end network benchmark: 3 validator nodes, each in its own Tokio task,
//! and a 4th task sending many TXs to node 0 over HTTP.
//!
//! Run: `cargo bench -p node --features testnet`
//! Stress only: `cargo bench -p node --features testnet -- stress`
//! Live dashboard: `DASHBOARD_PORT=3200 cargo bench ...`, then open the printed URL.
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
/// Stress ramp: short blocks, paced senders on every node; the offered rate
/// grows by STRESS_GROWTH per step until a step loses more than MAX_ERROR_RATE.
const STRESS_BLOCK_TIME: Duration = Duration::from_millis(10);
const STRESS_SENDERS: u64 = 12;
const STRESS_STEP: Duration = Duration::from_secs(2);
/// A TX counts as lost if it is not committed on every node this long after the load.
const STRESS_SETTLE: Duration = Duration::from_secs(5);
const STRESS_START_TPS: f64 = 5_000.0;
const STRESS_GROWTH: f64 = 1.5;
const STRESS_MAX_STEPS: usize = 12;
const MAX_ERROR_RATE: f64 = 0.01;
/// Measured runs per ramp step (1 with `--test`).
const STRESS_RUNS: usize = 3;

/// `DASHBOARD_PORT=3200`: every network listens on 3200.. so one browser tab
/// on the printed dashboard URL follows all runs live.
fn dashboard_port() -> Option<u16> {
    let port = std::env::var("DASHBOARD_PORT").ok()?;
    Some(port.parse().expect("DASHBOARD_PORT must be a port number"))
}

async fn spawn_network(block_time: Duration) -> Testnet {
    Testnet::spawn_on(VALIDATORS, block_time, dashboard_port())
        .await
        .expect("testnet starts")
}

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
    max_block_txs: u64,
    /// Accepted TXs not committed on every node before the deadline.
    lost: u64,
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
        run.max_block_txs = run.max_block_txs.max(stats.max_block_txs);
    }
}

/// Load from a 4th task until the 3 nodes agree on every accepted TX.
async fn load_until_synced(count: u64, rogue_blocks: u64) -> Run {
    load_until_synced_from(1, count, rogue_blocks).await
}

/// `senders` concurrent load tasks (one key each, `per_sender` TXs), spread
/// round-robin over the nodes: sender 0 targets node 0.
async fn load_until_synced_from(senders: u64, per_sender: u64, rogue_blocks: u64) -> Run {
    let network = spawn_network(BLOCK_TIME).await;
    let started = Instant::now();
    let loaders = spawn_loaders(&network, senders, per_sender, None);
    for _ in 0..rogue_blocks {
        network
            .send_bad_block(1)
            .await
            .expect("bad block is rejected");
    }
    let mut load = join_loaders(loaders).await;
    let loaded = started.elapsed();
    load.elapsed = loaded;
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

/// One load task per sender (own key, `per_sender` TXs, optional TX/s pace),
/// spread round-robin over the nodes: sender 0 targets node 0.
fn spawn_loaders(
    network: &Testnet,
    senders: u64,
    per_sender: u64,
    per_second: Option<f64>,
) -> Vec<tokio::task::JoinHandle<LoadReport>> {
    (0..senders)
        .map(|sender| {
            let url = network.http_url(sender as usize % network.len());
            tokio::spawn(async move {
                let load = Load {
                    sender: sender_key(sender),
                    count: per_sender,
                    batch_size: BATCH_SIZE,
                    forged_every: None,
                    per_second,
                };
                send_load(&url, load).await
            })
        })
        .collect()
}

async fn join_loaders(loaders: Vec<tokio::task::JoinHandle<LoadReport>>) -> LoadReport {
    let mut load = LoadReport::default();
    for loader in loaders {
        let report = loader.await.expect("load task");
        load.submitted += report.submitted;
        load.accepted += report.accepted;
        load.rejected += report.rejected;
        load.failed += report.failed;
    }
    load
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
            let network = spawn_network(BLOCK_TIME).await;
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
                    let mut network = spawn_network(BLOCK_TIME).await;
                    let load = Load {
                        sender: sender_key(CHAIN_TXS),
                        count: CHAIN_TXS,
                        batch_size: BATCH_SIZE,
                        forged_every: None,
                        per_second: None,
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

/// One ramp step: `offered_tps` for STRESS_STEP from STRESS_SENDERS paced
/// senders, then STRESS_SETTLE for every node to commit. Never panics on
/// overload: what is not committed everywhere by then is counted as lost.
async fn stress_step(offered_tps: f64) -> Run {
    let network = spawn_network(STRESS_BLOCK_TIME).await;
    let per_sender_tps = offered_tps / STRESS_SENDERS as f64;
    let per_sender = (per_sender_tps * STRESS_STEP.as_secs_f64()).round() as u64;
    let started = Instant::now();
    let loaders = spawn_loaders(&network, STRESS_SENDERS, per_sender, Some(per_sender_tps));
    let mut load = join_loaders(loaders).await;
    let loaded = started.elapsed();
    load.elapsed = loaded;
    // Timing out is the overload signal, not a failure of the benchmark.
    let _ = network
        .wait_until_synced(load.accepted, STRESS_SETTLE)
        .await;
    let sync = started.elapsed();
    let committed = network.committed_everywhere().await;
    let mut run = Run {
        sync,
        lag: sync - loaded,
        lost: load.accepted.saturating_sub(committed),
        load,
        ..Run::default()
    };
    network_totals(&network, &mut run).await;
    run
}

fn run_error_rate(runs: &[Run]) -> f64 {
    let submitted: u64 = runs.iter().map(|run| run.load.submitted).sum();
    let errors: u64 = runs.iter().map(run_errors).sum();
    if submitted == 0 {
        0.0
    } else {
        errors as f64 / submitted as f64
    }
}

fn run_errors(run: &Run) -> u64 {
    run.load.rejected + run.load.failed + run.lost
}

/// Raises the offered TX/s step by step and stops after the first step whose
/// error rate (rejected + failed + lost) exceeds MAX_ERROR_RATE.
///
/// Runs outside Criterion: its sampler needs at least 10 samples plus a warm-up
/// per step (~20 runs of up to 7 s each); `runs` (STRESS_RUNS) is enough here.
fn run_stress_ramp(runtime: &Runtime, runs: usize) {
    let mut offered = STRESS_START_TPS;
    for _ in 0..STRESS_MAX_STEPS {
        let tps = offered.round() as u64;
        let id = format!("stress/ramp/{tps:06}");
        for run in 1..=runs {
            let result = runtime.block_on(stress_step(offered));
            println!(
                "stress: {tps} TX/s, run {run}/{runs}: sync {:.0} ms, error rate {:.3}%",
                millis(result.sync),
                run_error_rate(std::slice::from_ref(&result)) * 100.0
            );
            record(&id, result);
        }
        let error_rate = run_error_rate(&RUNS.lock().unwrap()[&id]);
        println!(
            "stress: offered {tps} TX/s -> error rate {:.3}%",
            error_rate * 100.0
        );
        if error_rate > MAX_ERROR_RATE {
            println!(
                "stress: stopped, error rate above {}%",
                MAX_ERROR_RATE * 100.0
            );
            break;
        }
        offered *= STRESS_GROWTH;
    }
}

/// Runs per ramp step for this command line, or `None` when it excludes the
/// ramp: `-- stress` (or no filter) runs it, `--test` does 1 run per step.
fn stress_runs() -> Option<usize> {
    // Criterion options followed by a value, which is not a filter.
    const WITH_VALUE: [&str; 14] = [
        "--save-baseline",
        "--baseline",
        "--baseline-lenient",
        "--load-baseline",
        "--profile-time",
        "--measurement-time",
        "--warm-up-time",
        "--sample-size",
        "--noise-threshold",
        "--confidence-level",
        "--significance-level",
        "--nresamples",
        "--output-format",
        "--plotting-backend",
    ];
    let mut args = std::env::args().skip(1);
    let (mut filters, mut test) = (Vec::new(), false);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--list" => return None,
            "--test" => test = true,
            flag if WITH_VALUE.contains(&flag) => {
                args.next();
            }
            flag if flag.starts_with('-') => {}
            filter => filters.push(filter.to_string()),
        }
    }
    let selected = filters.is_empty()
        || filters
            .iter()
            .any(|f| "stress/ramp".contains(f.as_str()) || f.contains("stress"));
    selected.then_some(if test { 1 } else { STRESS_RUNS })
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
    /// Accepted TXs not committed everywhere before the deadline (stress only).
    lost_txs: u64,
    sync_resets: u64,
    bad_blocks: u64,
    resets_per_bad_block: Option<f64>,
    /// Resets that no rogue block explains (honest nodes out of sync).
    unexpected_resets: u64,
    avg_propagation_ms: Option<f64>,
    avg_finality_ms: Option<f64>,
    tps_to_sync: Option<f64>,
    max_block_txs: u64,
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
    let errors = sum(run_errors);
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
        lost_txs: sum(|run| run.lost),
        sync_resets,
        bad_blocks,
        resets_per_bad_block: (bad_blocks > 0).then(|| sync_resets as f64 / bad_blocks as f64),
        unexpected_resets: sync_resets.saturating_sub(bad_blocks),
        avg_propagation_ms: mean(&all(|run| &run.propagation_ms)),
        avg_finality_ms: mean(&all(|run| &run.finality_ms)),
        tps_to_sync: (accepted > 0 && total_sync > 0.0).then(|| accepted as f64 / total_sync),
        max_block_txs: runs.iter().map(|run| run.max_block_txs).max().unwrap_or(0),
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
            "  txs {}/{} accepted | error rate {:.4} | failed {} | lost {} | resets {} (bad blocks {}, per bad block {}, unexpected {})",
            s.txs_accepted,
            s.txs_submitted,
            s.error_rate,
            s.failed_requests_txs,
            s.lost_txs,
            s.sync_resets,
            s.bad_blocks,
            opt(s.resets_per_bad_block),
            s.unexpected_resets
        );
        println!(
            "  propagation {} ms | finality {} ms | tps to sync {} | biggest block {} txs",
            opt(s.avg_propagation_ms),
            opt(s.avg_finality_ms),
            opt(s.tps_to_sync),
            s.max_block_txs
        );
    }
    let sustained = summaries
        .iter()
        .filter(|(id, s)| id.starts_with("stress/ramp/") && s.error_rate <= MAX_ERROR_RATE)
        .filter_map(|(_, s)| s.tps_to_sync)
        .fold(None, |best: Option<f64>, tps| {
            Some(best.map_or(tps, |b| b.max(tps)))
        });
    if let Some(tps) = sustained {
        println!(
            "stress: best TPS to sync with error rate <= {}%: {tps:.0} (block time {STRESS_BLOCK_TIME:?})",
            MAX_ERROR_RATE * 100.0
        );
    }
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"));
    let path = target.join("criterion/network_stats.json");
    let written =
        std::fs::create_dir_all(path.parent().expect("file has a parent")).and_then(|()| {
            std::fs::write(
                &path,
                serde_json::to_vec_pretty(&summaries).expect("serializable"),
            )
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
    if let Some(base) = dashboard_port() {
        let nodes: Vec<_> = (0..VALIDATORS as u16)
            .map(|node| format!("http://127.0.0.1:{}", base + node))
            .collect();
        println!("dashboard: {}/?nodes={}", nodes[0], nodes.join(","));
    }
    let mut criterion = Criterion::default().configure_from_args();
    bench_sync(&mut criterion, &runtime);
    bench_bad_sync(&mut criterion, &runtime);
    bench_catch_up(&mut criterion, &runtime);
    criterion.final_summary();
    if let Some(runs) = stress_runs() {
        run_stress_ramp(&runtime, runs);
    }
    print_report();
}
