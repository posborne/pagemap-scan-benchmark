use clap::Parser;
use nix::sys::mman::{mmap_anonymous, MapFlags, ProtFlags};
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

    /// Size of memory to scan for dirty pages when using pagemap_scan; if
    /// all pages in this range are dirty, madvise will be issued on the
    /// full range.  (e.g. 128M, 256M, 1G)
    #[arg(short = 'r', long, default_value = "128M")]
    keep_resident: String,

    /// Fraction of memory to dirty (0.0 to 1.0)
    #[arg(short = 'd', long, default_value_t = 0.1)]
    dirty_fraction: f64,

    /// Suppress normal output in favor of JSON
    #[arg(short = 'j', long, action)]
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

#[derive(Serialize, Debug)]
struct BenchResult {
    pub strategy: Strategy,
    pub total_size: usize,
    pub keep_resident: usize,
    pub dirty_fraction: f64,
    pub duration: Duration,
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
    let keep_resident_size = parse_size(&args.keep_resident)?;
    let dirty_fraction = args.dirty_fraction;
    let quiet = args.json;

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
    qprintln!(quiet, "Keep Resident / Scan: {keep_resident_size}");
    qprintln!(quiet, "------------------------------\n");

    let mut results: Vec<BenchResult> = Vec::new();
    for _ in 0..args.iterations {
        results.push(run_benchmark_memset(
            total_size,
            dirty_fraction,
            keep_resident_size,
            quiet,
        )?);
        results.push(run_benchmark_madvise(
            total_size,
            dirty_fraction,
            keep_resident_size,
            quiet,
        )?);
        results.push(run_benchmark_pagemap_scan(
            total_size,
            dirty_fraction,
            keep_resident_size,
            quiet,
        )?);
    }

    if args.json {
        println!("{}", serde_json::to_string(&results)?);
    }

    Ok(())
}

/// Allocates and dirties memory for a test scenario.
fn setup_memory(
    total_size: usize,
    keep_resident_size: usize,
    dirty_fraction: f64,
) -> anyhow::Result<*mut u8> {
    let prot = ProtFlags::PROT_READ | ProtFlags::PROT_WRITE;
    let flags = MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS;
    let map = unsafe { mmap_anonymous(None, total_size.try_into()?, prot, flags) }?;
    let map = map.as_ptr() as *mut u8;

    // Trigger page fault on the keep_resident bytes prior to the test
    // so it isn't skewing the measurements
    let keep_res_slice = unsafe { slice::from_raw_parts_mut(map, keep_resident_size) };
    keep_res_slice.fill(0);

    // Dirty a fraction of the memory
    let dirty_bytes = (total_size as f64 * dirty_fraction).round() as usize;
    if dirty_bytes > 0 {
        let dirty_slice = unsafe { slice::from_raw_parts_mut(map, dirty_bytes) };
        dirty_slice.fill(1); // Write something to make pages dirty
    }

    Ok(map)
}

fn run_benchmark_memset(
    total_size: usize,
    dirty_fraction: f64,
    keep_resident_size: usize,
    quiet: bool,
) -> anyhow::Result<BenchResult> {
    qprintln!(quiet, "Scenario 1: Naive memset");
    let map = setup_memory(total_size, keep_resident_size, dirty_fraction)?;
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
        keep_resident: keep_resident_size,
        dirty_fraction,
        duration,
    })
}

fn run_benchmark_madvise(
    total_size: usize,
    dirty_fraction: f64,
    keep_resident_size: usize,
    quiet: bool,
) -> anyhow::Result<BenchResult> {
    qprintln!(
        quiet,
        "Scenario 2: memzero of keep_resident + madvise(MADV_DONTNEED) remaining"
    );
    let map = setup_memory(total_size, keep_resident_size, dirty_fraction)?;

    let start = Instant::now();

    // zero keep_resident bytes
    let kr_slice = unsafe { slice::from_raw_parts_mut(map, keep_resident_size) };
    kr_slice.fill(0);

    let rem_ptr = unsafe { map.offset(keep_resident_size as isize) };
    let ret = unsafe {
        libc::madvise(
            rem_ptr as *mut libc::c_void,
            total_size - keep_resident_size,
            libc::MADV_DONTNEED,
        )
    };
    let duration = start.elapsed();

    if ret != 0 {
        return Err(std::io::Error::last_os_error().into());
    }

    qprintln!(quiet, "  Zeroed {keep_resident_size} bytes.");
    qprintln!(
        quiet,
        "  Called madvise on {} bytes.",
        total_size - keep_resident_size
    );
    qprintln!(quiet, "  Time taken: {:?}\n", duration);

    unsafe { libc::munmap(map as *mut libc::c_void, total_size) };

    Ok(BenchResult {
        strategy: Strategy::Madvise,
        total_size,
        keep_resident: keep_resident_size,
        dirty_fraction,
        duration,
    })
}

fn run_benchmark_pagemap_scan(
    total_size: usize,
    dirty_fraction: f64,
    keep_resident_size: usize,
    quiet: bool,
) -> anyhow::Result<BenchResult> {
    // TODO: add some page size/alignment adjustments
    qprintln!(
        quiet,
        "Scenario 3: PAGEMAP_SCAN + targeted memset + madvise(MADV_DONTNEED) if beyond barrier"
    );
    let map = setup_memory(total_size, keep_resident_size, dirty_fraction)?;
    let pages = total_size / rustix::param::page_size();

    let start = Instant::now();

    let mut regions: Box<[MaybeUninit<pagemap::PageRegion>]> = Box::new_uninit_slice(pages);
    let dirty_pages = pagemap::dirty_pages_in_region(map, keep_resident_size, regions.as_mut())?;

    // println!("Dirty Pages: {dirty_pages:?}");
    let mut total_zeroed = 0;
    for dirty_region in dirty_pages.regions {
        let start_ptr = dirty_region.start as *mut u8;
        let len = usize::try_from(dirty_region.end - dirty_region.start)?;
        let region_slice = unsafe { slice::from_raw_parts_mut(start_ptr, len) };
        region_slice.fill(0);
        total_zeroed += len;
    }

    if total_zeroed == keep_resident_size {
        // madvise the remainder
        qprintln!(quiet, "  Had to madvise the full range");
        let rem_ptr = unsafe { map.offset(isize::try_from(keep_resident_size)?) };
        let ret = unsafe {
            libc::madvise(
                rem_ptr as *mut libc::c_void,
                total_size - keep_resident_size,
                libc::MADV_DONTNEED,
            )
        };

        if ret != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
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
        keep_resident: keep_resident_size,
        dirty_fraction,
        duration,
    })
}
