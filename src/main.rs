use clap::Parser;
use nix::sys::mman::{mmap_anonymous, MapFlags, ProtFlags};
use rayon::prelude::*;
use serde::Serialize;
use std::marker::PhantomData;
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
}

#[derive(Debug)]
struct BenchArgs {
    total_size: usize,
    dirty_fraction: f64,
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

impl BenchResult {
    fn new(args: &BenchArgs, strategy: Strategy, duration: Duration) -> Self {
        let BenchArgs {
            total_size,
            dirty_fraction,
            threads,
            processes,
            ..
        } = *args;
        BenchResult {
            strategy,
            total_size,
            dirty_fraction,
            duration,
            threads,
            processes,
        }
    }
}

struct MemoryRegion<'a> {
    ptr: *mut u8,
    size: usize,
    dirty_pct: f64,
    phantom: PhantomData<&'a [u8]>,
}

impl<'a> MemoryRegion<'a> {
    pub fn new(size: usize, dirty_pct: f64, force_resident: bool) -> anyhow::Result<Self> {
        let prot = ProtFlags::PROT_READ | ProtFlags::PROT_WRITE;
        let flags = MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS;
        let map = unsafe { mmap_anonymous(None, size.try_into()?, prot, flags) }?;
        let map = map.as_ptr() as *mut u8;

        if force_resident {
            let keep_res_slice = unsafe { slice::from_raw_parts_mut(map, size) };
            keep_res_slice.fill(0);
        }

        Ok(MemoryRegion {
            ptr: map,
            size,
            dirty_pct,
            phantom: PhantomData,
        })
    }

    pub fn as_mut_slice(&mut self) -> &'a mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr, self.size) }
    }

    pub fn make_dirty(&mut self) {
        let dirty_bytes = (self.size as f64 * self.dirty_pct).round() as usize;
        if dirty_bytes > 0 {
            let dirty_slice = unsafe { slice::from_raw_parts_mut(self.ptr, dirty_bytes) };
            dirty_slice.fill(0xAA);
        }
    }
}

impl<'a> Drop for MemoryRegion<'a> {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.ptr as *mut libc::c_void, self.size) };
    }
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

    // we want to reduce the number of new regions we create
    // while still creating enough work to be meaningful
    let do_memset = || -> anyhow::Result<Vec<BenchResult>> {
        let mut region = MemoryRegion::new(total_size, args.dirty_fraction, true)?;
        (0..args.iterations)
            .map(|_i| run_benchmark_memset(&bench_args, &mut region))
            .collect::<anyhow::Result<Vec<BenchResult>>>()
    };

    let do_madvise = || {
        let mut region = MemoryRegion::new(total_size, args.dirty_fraction, false)?;
        (0..args.iterations)
            .map(|_i| run_benchmark_madvise(&bench_args, &mut region))
            .collect::<anyhow::Result<Vec<BenchResult>>>()
    };

    let do_pagemap_scan = || {
        let mut region = MemoryRegion::new(total_size, args.dirty_fraction, false)?;
        (0..args.iterations)
            .map(|_i| run_benchmark_pagemap_scan(&bench_args, &mut region))
            .collect::<anyhow::Result<Vec<BenchResult>>>()
    };

    let results: Vec<BenchResult> = (0..args.threads)
        .into_par_iter()
        .map(|_| [do_memset(), do_madvise(), do_pagemap_scan()])
        .flatten()
        .flatten()
        .flatten()
        .collect();

    if args.json {
        println!("{}", serde_json::to_string(&results)?);
    }

    Ok(())
}

fn run_benchmark_memset(
    args: &BenchArgs,
    region: &mut MemoryRegion,
) -> anyhow::Result<BenchResult> {
    let start = Instant::now();
    region.make_dirty();
    region.as_mut_slice().fill(0);
    let duration = start.elapsed();

    Ok(BenchResult::new(args, Strategy::MemZero, duration))
}

fn run_benchmark_madvise(
    args: &BenchArgs,
    region: &mut MemoryRegion,
) -> anyhow::Result<BenchResult> {
    let start = Instant::now();
    region.make_dirty();
    let ret = unsafe {
        libc::madvise(
            region.ptr as *mut libc::c_void,
            args.total_size,
            libc::MADV_DONTNEED,
        )
    };
    let duration = start.elapsed();

    if ret != 0 {
        return Err(std::io::Error::last_os_error().into());
    }

    Ok(BenchResult::new(args, Strategy::Madvise, duration))
}

fn run_benchmark_pagemap_scan(
    args: &BenchArgs,
    region: &mut MemoryRegion,
) -> anyhow::Result<BenchResult> {
    let pages = args.total_size / rustix::param::page_size();

    let start = Instant::now();
    region.make_dirty();
    let mut regions: Box<[MaybeUninit<pagemap::PageRegion>]> = Box::new_uninit_slice(pages);
    let dirty_pages =
        pagemap::dirty_pages_in_region(region.ptr, args.total_size, regions.as_mut())?;
    for dirty_region in dirty_pages.regions {
        let start_ptr = dirty_region.start as *mut u8;
        let len = usize::try_from(dirty_region.end - dirty_region.start)?;
        let region_slice = unsafe { slice::from_raw_parts_mut(start_ptr, len) };
        region_slice.fill(0);
    }
    let duration = start.elapsed();

    Ok(BenchResult::new(args, Strategy::PagemapScan, duration))
}
