use clap::Parser;
use nix::sys::mman::{mmap_anonymous, MapFlags, ProtFlags};
use rayon::prelude::*;
use serde::Serialize;
use std::mem::MaybeUninit;
use std::slice;
use std::time::{Duration, Instant};

mod pagemap;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Total size of the memory mapping (e.g., 1G, 512M, 1024K)
    #[arg(short = 's', long, default_value = "1G")]
    size: String,

    /// Fraction of memory to dirty (0.0 to 1.0)
    #[arg(short = 'd', long, default_value_t = 0.1)]
    dirty_fraction: f64,

    /// Parallel threads to run
    #[arg(short = 't', long, default_value_t = 1)]
    threads: usize,

    /// Parallel processes being run (just for documentation)
    #[arg(short = 'p', long, default_value_t = 1)]
    processes: usize,

    /// Suppress normal output in favor of JSON
    #[arg(long, action)]
    json: bool,

    /// Iterations to run
    #[arg(short = 'i', long, default_value = "1")]
    iterations: u64,
}

#[derive(Serialize, Debug)]
enum Strategy {
    MemZero,
    Madvise,
    PagemapScan,
    Heuristic,
}

#[derive(Debug)]
struct BenchArgs {
    total_size: usize,
    dirty_fraction: f64,
    quiet: bool,
    threads: usize,
    processes: usize,
}

#[derive(Serialize, Debug)]
struct BenchResult {
    pub strategy: Strategy,
    pub total_size: usize,
    pub dirty_fraction: f64,
    pub duration: Duration,
    pub threads: usize,
    pub processes: usize,
}

macro_rules! qprintln {
    ($condition:expr, $($arg:tt)*) => {
        if !$condition {
            println!($($arg)*);
        }
    };
}

fn parse_size(size_str: &str) -> anyhow::Result<usize> {
    let s = size_str.to_uppercase();
    let (num_str, mult_char) = s.split_at(s.len() - 1);
    let num = num_str
        .parse::<usize>()
        .map_err(|_| anyhow::anyhow!("Invalid number: {}", num_str))?;
    let mult = match mult_char {
        "K" => 1024,
        "M" => 1024 * 1024,
        "G" => 1024 * 1024 * 1024,
        _ => {
            return Err(anyhow::anyhow!(
                "Invalid size suffix: {}. Use K, M, or G.",
                mult_char
            ))
        }
    };
    Ok(num * mult)
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let total_size = parse_size(&args.size)?;
    let dirty_fraction = args.dirty_fraction;
    let quiet = args.json;

    let bench_args = BenchArgs {
        total_size,
        dirty_fraction,
        quiet,
        threads: args.threads,
        processes: args.processes,
    };

    if !(0.0..=1.0).contains(&dirty_fraction) {
        return Err(anyhow::anyhow!(
            "Dirty fraction must be between 0.0 and 1.0"
        ));
    }

    qprintln!(quiet, "--- PAGEMAP_SCAN Benchmark ---");
    qprintln!(
        quiet,
        "Total Memory Size: {:.2} MiB",
        total_size as f64 / 1024.0 / 1024.0
    );
    qprintln!(
        quiet,
        "Dirty Fraction: {:.2}% ({:.0} bytes)",
        dirty_fraction * 100.0,
        dirty_fraction * total_size as f64
    );
    qprintln!(quiet, "------------------------------\n");

    rayon::ThreadPoolBuilder::new()
        .num_threads(args.threads)
        .build_global()?;

    let results = (0..args.iterations)
        .into_par_iter()
        .map(|_i| {
            [
                run_benchmark_memset(&bench_args),
                run_benchmark_madvise(&bench_args),
                run_benchmark_pagemap_scan(&bench_args),
                run_benchmark_heuristic(&bench_args),
            ]
        })
        .flatten()
        .collect::<anyhow::Result<Vec<BenchResult>>>()?;

    if args.json {
        println!("{}", serde_json::to_string(&results)?);
    }

    Ok(())
}

/// Allocates and dirties memory for a test scenario.
fn setup_memory(total_size: usize, dirty_fraction: f64, warmup: bool) -> anyhow::Result<*mut u8> {
    let prot = ProtFlags::PROT_READ | ProtFlags::PROT_WRITE;
    let flags = MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS;
    let map = unsafe { mmap_anonymous(None, total_size.try_into()?, prot, flags) }?;
    let map = map.as_ptr() as *mut u8;

    if warmup {
        let keep_res_slice = unsafe { slice::from_raw_parts_mut(map, total_size) };
        keep_res_slice.fill(0);
    }

    // Dirty a fraction of the memory
    let dirty_bytes = (total_size as f64 * dirty_fraction).round() as usize;
    if dirty_bytes > 0 {
        let dirty_slice = unsafe { slice::from_raw_parts_mut(map, dirty_bytes) };
        dirty_slice.fill(1); // Write something to make pages dirty
    }

    Ok(map)
}

fn run_benchmark_memset(args: &BenchArgs) -> anyhow::Result<BenchResult> {
    let BenchArgs {
        total_size,
        dirty_fraction,
        quiet,
        threads,
        processes,
    } = *args;
    qprintln!(quiet, "Scenario 1: Naive memset on all pages");
    let map = setup_memory(total_size, dirty_fraction, true)?;
    let slice = unsafe { slice::from_raw_parts_mut(map, total_size) };

    let start = Instant::now();
    slice.fill(0);
    let duration = start.elapsed();

    qprintln!(quiet, "  Zeroed all {} bytes.", total_size);
    qprintln!(quiet, "  Time taken: {:?}\n", duration);

    unsafe { libc::munmap(map as *mut libc::c_void, total_size) };

    Ok(BenchResult {
        strategy: Strategy::MemZero,
        total_size,
        dirty_fraction,
        duration,
        threads,
        processes,
    })
}

fn run_benchmark_madvise(args: &BenchArgs) -> anyhow::Result<BenchResult> {
    let BenchArgs {
        total_size,
        dirty_fraction,
        quiet,
        threads,
        processes,
    } = *args;
    qprintln!(quiet, "Scenario 2: use madvise on all pages");
    let map = setup_memory(total_size, dirty_fraction, false)?;

    let start = Instant::now();
    let ret = unsafe { libc::madvise(map as *mut libc::c_void, total_size, libc::MADV_DONTNEED) };
    let duration = start.elapsed();

    if ret != 0 {
        return Err(std::io::Error::last_os_error().into());
    }

    qprintln!(quiet, "  Called madvise on {total_size} bytes.");
    qprintln!(quiet, "  Time taken: {:?}\n", duration);

    unsafe { libc::munmap(map as *mut libc::c_void, total_size) };

    Ok(BenchResult {
        strategy: Strategy::Madvise,
        total_size,
        dirty_fraction,
        duration,
        threads,
        processes,
    })
}

fn run_benchmark_pagemap_scan(args: &BenchArgs) -> anyhow::Result<BenchResult> {
    let BenchArgs {
        total_size,
        dirty_fraction,
        quiet,
        threads,
        processes,
    } = *args;
    qprintln!(quiet, "Scenario 3: Only memset dirty pages");
    assert_eq!(total_size % rustix::param::page_size(), 0);
    let map = setup_memory(total_size, dirty_fraction, false)?;
    let pages = total_size / rustix::param::page_size();

    let start = Instant::now();

    let mut regions: Box<[MaybeUninit<pagemap::PageRegion>]> = Box::new_uninit_slice(pages);
    let dirty_pages = pagemap::dirty_pages_in_region(map, total_size, regions.as_mut())?;

    let mut total_zeroed = 0;
    for dirty_region in dirty_pages.regions {
        let start_ptr = dirty_region.start as *mut u8;
        let len = usize::try_from(dirty_region.end - dirty_region.start)?;
        let region_slice = unsafe { slice::from_raw_parts_mut(start_ptr, len) };
        region_slice.fill(0);
        total_zeroed += len;
    }
    let duration = start.elapsed();

    qprintln!(
        quiet,
        "  Found {} dirty page ranges.",
        dirty_pages.regions.len()
    );
    qprintln!(quiet, "  Zeroed {} bytes.", total_zeroed);
    qprintln!(quiet, "  Time taken: {:?}\n", duration);

    unsafe { libc::munmap(map as *mut libc::c_void, total_size) };

    Ok(BenchResult {
        strategy: Strategy::PagemapScan,
        total_size,
        dirty_fraction,
        duration,
        threads,
        processes,
    })
}

fn run_benchmark_heuristic(args: &BenchArgs) -> anyhow::Result<BenchResult> {
    let BenchArgs {
        total_size, quiet, ..
    }: BenchArgs = *args;
    qprintln!(
        quiet,
        "Scenario 4: Try to do the fastest thing using heuristics"
    );

    let mut bench_result = if total_size <= 128 * 1024 {
        // for small regions, avoid the syscall
        run_benchmark_memset(args)?
    } else if total_size >= 1 * 1024 * 1024 {
        // for large regions, use madvise so we don't keep
        // tons of memory resident
        run_benchmark_madvise(args)?
    } else {
        run_benchmark_pagemap_scan(args)?
    };

    bench_result.strategy = Strategy::Heuristic;
    Ok(bench_result)
}
