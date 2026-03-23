//! Performance benchmarks for kraken.
//!
//! Verifies performance targets from SPECS.md:
//! - Single-node point lookups: < 1ms
//! - Single-node range scans: > 10K rows/sec
//! - Write throughput: > 1K inserts/sec
//!
//! Run with: cargo bench

use criterion::{criterion_group, criterion_main, Criterion, Throughput, BenchmarkId};
use std::time::Duration;
use tempfile::TempDir;

use kraken::executor::ExecutionEngine;

/// Setup a database with test data.
fn setup_database(row_count: usize) -> (ExecutionEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let engine = ExecutionEngine::open(temp_dir.path()).unwrap();
    
    // Create table
    engine.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, value TEXT, num INTEGER)").unwrap();
    
    // Insert rows in batches
    for i in 0..row_count {
        engine.execute(&format!(
            "INSERT INTO bench VALUES ({}, 'value_{}', {})",
            i, i, i * 10
        )).unwrap();
    }
    
    engine.flush().unwrap();
    (engine, temp_dir)
}

/// Benchmark point lookups by primary key.
fn bench_point_lookup(c: &mut Criterion) {
    let (engine, _temp_dir) = setup_database(1000);
    
    let mut group = c.benchmark_group("point_lookup");
    group.measurement_time(Duration::from_secs(5));
    
    group.bench_function("lookup_by_id", |b| {
        let mut id = 0;
        b.iter(|| {
            let query = format!("SELECT * FROM bench WHERE id = {}", id);
            let result = engine.execute(&query).unwrap();
            assert_eq!(result.rows.len(), 1);
            id = (id + 1) % 1000;
        });
    });
    
    group.finish();
}

/// Benchmark range scans.
fn bench_range_scan(c: &mut Criterion) {
    let (engine, _temp_dir) = setup_database(10000);
    
    let mut group = c.benchmark_group("range_scan");
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(10000));
    
    group.bench_function("full_table_scan", |b| {
        b.iter(|| {
            let result = engine.execute("SELECT * FROM bench").unwrap();
            assert_eq!(result.rows.len(), 10000);
        });
    });
    
    group.bench_function("filtered_scan", |b| {
        b.iter(|| {
            let result = engine.execute("SELECT * FROM bench WHERE num > 50000").unwrap();
            assert!(result.rows.len() > 0);
        });
    });
    
    group.finish();
}

/// Benchmark write throughput.
fn bench_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_throughput");
    group.measurement_time(Duration::from_secs(10));
    
    for batch_size in [100, 500, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        
        group.bench_with_input(
            BenchmarkId::new("insert_batch", batch_size),
            batch_size,
            |b, &size| {
                let temp_dir = TempDir::new().unwrap();
                let engine = ExecutionEngine::open(temp_dir.path()).unwrap();
                engine.execute("CREATE TABLE bench (id INTEGER, value TEXT)").unwrap();
                
                let mut counter = 0;
                b.iter(|| {
                    for _ in 0..size {
                        engine.execute(&format!(
                            "INSERT INTO bench VALUES ({}, 'value_{}')",
                            counter, counter
                        )).unwrap();
                        counter += 1;
                    }
                    engine.flush().unwrap();
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmark mixed workload (reads + writes).
fn bench_mixed_workload(c: &mut Criterion) {
    let (engine, _temp_dir) = setup_database(5000);
    
    let mut group = c.benchmark_group("mixed_workload");
    group.measurement_time(Duration::from_secs(5));
    
    // 80% reads, 20% writes
    group.bench_function("80_read_20_write", |b| {
        let mut counter = 5000;
        let mut op = 0;
        b.iter(|| {
            if op % 5 == 0 {
                // Write
                engine.execute(&format!(
                    "INSERT INTO bench VALUES ({}, 'new_{}', {})",
                    counter, counter, counter * 10
                )).unwrap();
                counter += 1;
            } else {
                // Read
                let id = op % 5000;
                let result = engine.execute(&format!(
                    "SELECT * FROM bench WHERE id = {}", id
                )).unwrap();
                assert!(result.rows.len() <= 1);
            }
            op += 1;
        });
    });
    
    group.finish();
}

/// Benchmark aggregations.
fn bench_aggregation(c: &mut Criterion) {
    let (engine, _temp_dir) = setup_database(10000);
    
    let mut group = c.benchmark_group("aggregation");
    group.measurement_time(Duration::from_secs(5));
    
    group.bench_function("count_all", |b| {
        b.iter(|| {
            let result = engine.execute("SELECT COUNT(id) FROM bench").unwrap();
            assert_eq!(result.rows.len(), 1);
        });
    });
    
    group.bench_function("sum_and_avg", |b| {
        b.iter(|| {
            let result = engine.execute("SELECT SUM(num), AVG(num) FROM bench").unwrap();
            assert_eq!(result.rows.len(), 1);
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_point_lookup,
    bench_range_scan,
    bench_write_throughput,
    bench_mixed_workload,
    bench_aggregation,
);

criterion_main!(benches);
