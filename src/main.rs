use clap::Parser;
use nix::sys::mman::{mmap_anonymous, MapFlags, ProtFlags};
use std::mem::MaybeUninit;
use std::slice;
use std::time::Instant;

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
    scanned_size: String,

    /// Fraction of memory to dirty (0.0 to 1.0)
    #[arg(short = 'd', long, default_value_t = 0.1)]
    dirty_fraction: f64,
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
    let scanned_size = parse_size(&args.scanned_size)?;
    let dirty_fraction = args.dirty_fraction;

    if !(0.0..=1.0).contains(&dirty_fraction) {
        return Err(anyhow::anyhow!(
            "Dirty fraction must be between 0.0 and 1.0"
        ));
    }

    println!("--- PAGEMAP_SCAN Benchmark ---");
    println!(
        "Total Memory Size: {:.2} MiB",
        total_size as f64 / 1024.0 / 1024.0
    );
    println!("Dirty Fraction: {:.2}%", dirty_fraction * 100.0);
    println!("------------------------------\n");

    run_benchmark_memset(total_size, dirty_fraction)?;
    run_benchmark_madvise(total_size, dirty_fraction)?;
    run_benchmark_pagemap_scan(total_size, dirty_fraction, scanned_size)?;

    Ok(())
}

/// Allocates and dirties memory for a test scenario.
fn setup_memory(total_size: usize, dirty_fraction: f64) -> anyhow::Result<*mut u8> {
    let prot = ProtFlags::PROT_READ | ProtFlags::PROT_WRITE;
    let flags = MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS;
    let map = unsafe { mmap_anonymous(None, total_size.try_into()?, prot, flags) }?;
    let map = map.as_ptr() as *mut u8;

    // Dirty a fraction of the memory
    let dirty_bytes = (total_size as f64 * dirty_fraction).round() as usize;
    if dirty_bytes > 0 {
        let dirty_slice = unsafe { slice::from_raw_parts_mut(map, dirty_bytes) };
        dirty_slice.fill(1); // Write something to make pages dirty
    }

    Ok(map)
}

fn run_benchmark_memset(total_size: usize, dirty_fraction: f64) -> anyhow::Result<()> {
    println!("Scenario 1: Naive memset");
    let map = setup_memory(total_size, dirty_fraction)?;
    let slice = unsafe { slice::from_raw_parts_mut(map, total_size) };

    let start = Instant::now();
    slice.fill(0);
    let duration = start.elapsed();

    println!("  Zeroed all {} bytes.", total_size);
    println!("  Time taken: {:?}\n", duration);

    unsafe { libc::munmap(map as *mut libc::c_void, total_size) };
    Ok(())
}

fn run_benchmark_madvise(total_size: usize, dirty_fraction: f64) -> anyhow::Result<()> {
    println!("Scenario 2: madvise(MADV_DONTNEED)");
    let map = setup_memory(total_size, dirty_fraction)?;

    let start = Instant::now();
    let ret = unsafe { libc::madvise(map as *mut libc::c_void, total_size, libc::MADV_DONTNEED) };
    let duration = start.elapsed();

    if ret != 0 {
        return Err(std::io::Error::last_os_error().into());
    }

    println!("  Called madvise on all {} bytes.", total_size);
    println!("  Time taken: {:?}\n", duration);

    unsafe { libc::munmap(map as *mut libc::c_void, total_size) };
    Ok(())
}

fn run_benchmark_pagemap_scan(
    total_size: usize,
    dirty_fraction: f64,
    scanned_size: usize,
) -> anyhow::Result<()> {
    println!("Scenario 3: PAGEMAP_SCAN + targeted memset");
    let map = setup_memory(total_size, dirty_fraction)?;
    // let map_addr = map as u64;
    let pages = total_size / rustix::param::page_size();

    let start = Instant::now();

    let mut regions: Box<[MaybeUninit<pagemap::PageRegion>]> = Box::new_uninit_slice(pages);
    let dirty_pages = pagemap::dirty_pages_in_region(map, scanned_size, regions.as_mut())?;

    // println!("Dirty Pages: {dirty_pages:?}");
    let mut total_zeroed = 0;
    for dirty_region in dirty_pages.regions {
        let start_ptr = dirty_region.start as *mut u8;
        let len = usize::try_from(dirty_region.end - dirty_region.start)?;
        let region_slice = unsafe { slice::from_raw_parts_mut(start_ptr, len) };
        region_slice.fill(0);
        total_zeroed += len;
    }

    if total_zeroed == scanned_size {
        // madvise the remainder
        println!("  Had to madvise the full range");
        let rem_ptr = unsafe { map.offset(isize::try_from(scanned_size)?) };
        let _ret = unsafe {
            libc::madvise(
                rem_ptr as *mut libc::c_void,
                total_size - scanned_size,
                libc::MADV_DONTNEED,
            )
        };
    }

    let duration = start.elapsed();

    println!("  Found {} dirty page ranges.", dirty_pages.regions.len());
    println!("  Zeroed {} bytes.", total_zeroed);
    println!("  Time taken: {:?}\n", duration);

    unsafe { libc::munmap(map as *mut libc::c_void, total_size) };
    Ok(())
}
