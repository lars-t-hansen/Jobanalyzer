// Compute system load aggregates from a set of log entries.

// TODO:
//
// - For some listings it may be desirable to print a heading?

use crate::configs;
use crate::{LoadFilterArgs,LoadPrintArgs,MetaArgs};

use anyhow::{bail,Result};
use chrono::prelude::{DateTime,NaiveDate};
use chrono::{Datelike,Timelike,Utc};
use sonarlog;
use std::collections::HashSet;

// Fields that can be printed for `--load`.
//
// Note that GPU memory is tricky.  On NVidia, the "percentage" is unreliable, while on AMD, the
// absolute value is unobtainable (on our current systems).  RVmemGB and RVmemPct represent the same
// value computed in two different ways from different base data, and though they should be the same
// they are usually not.

enum LoadFmt {
    Date,                       // YYYY-MM-DD
    Time,                       // HH:SS
    DateTime,                   // YYYY-MM-DD HH:SS
    CpuPct,                     // Accumulated CPU percentage, 100==1 core
    RCpuPct,                    // Accumulated CPU percentage, 100==all cores
    MemGB,                      // Accumulated memory usage, GB
    RMemGB,                     // Accumulated memory usage percentage, 100==all memory
    GpuPct,                     // Accumulated GPU percentage, 100==1 card
    RGpuPct,                    // Accumulated GPU percentage, 100==all cards
    VmemGB,                     // Accumulated GPU memory usage, GB
    RVmemGB,                    // Accumulated GPU memory usage percentage, 100==all memory
    VmemPct,                    // Accumulated GPU memory usage percentage, 100==1 card
    RVmemPct,                   // Accumulated GPU memory usage percentage, 100==all memory
    GpuMask                     // Accumulated GPUs in use
}

#[derive(Debug)]
struct LoadAggregate {
    cpu_pct: usize,
    mem_gb: usize,
    gpu_pct: usize,
    gpu_mem_pct: usize,
    gpu_mem_gb: usize,
    gpus: Option<HashSet<u32>>,
}

#[derive(PartialEq,Clone,Copy)]
enum BucketOpt {
    None,
    Hourly,
    Daily
}

#[derive(PartialEq,Clone,Copy)]
enum PrintOpt {
    All,
    Last
}

// We read and filter sonar records, bucket by host, sort by ascending timestamp, and then bucket by
// timestamp.  The buckets can then be aggregated into a "load" value for each time, which can in
// turn be averaged for a span of times.

pub fn aggregate_and_print_load(
    include_hosts: &HashSet<String>,
    filter_args: &LoadFilterArgs,
    print_args: &LoadPrintArgs,
    meta_args: &MetaArgs,
    by_host: &[(String, Vec<(DateTime<Utc>, Vec<sonarlog::LogEntry>)>)]) -> Result<()>
{
    let bucket_opt =
        if filter_args.daily {
            BucketOpt::Daily
        } else if filter_args.none {
            BucketOpt::None
        } else {
            BucketOpt::Hourly   // Default
        };

    let print_opt =
        if print_args.last {
            PrintOpt::Last
        } else {
            PrintOpt::All       // Default
        };

    let (fmt, relative) = compute_format(print_args)?;

    let config = if relative {
        if print_args.config_file.is_none() {
            bail!("Relative values requested without config file");
        }
        let config_filename = print_args.config_file.as_ref().unwrap();
        Some(configs::read_from_json(&config_filename)?)
    } else {
        None
    };

    // by_host is sorted ascending by hostname (outer string) and time (inner timestamp)

    for (hostname, records) in by_host {
        // We always print host name unless there's only one and it was selected explicitly.
        if include_hosts.len() != 1 {
            println!("HOST: {}", hostname);
        }
        let sysconf = if let Some(ref ht) = config {
            ht.get(hostname)
        } else {
            None
        };
            
        if bucket_opt != BucketOpt::None {
            let by_timeslot = aggregate_by_timeslot(bucket_opt, &filter_args.command, records);
            if print_opt == PrintOpt::All {
                for (timestamp, avg) in by_timeslot {
                    print_load(&fmt, sysconf, meta_args.verbose, &vec![], timestamp, &avg);
                }
            } else {
                let (timestamp, ref avg) = by_timeslot[by_timeslot.len()-1];
                print_load(&fmt, sysconf, meta_args.verbose, &vec![], timestamp, &avg);
            }
        }
        else if print_opt == PrintOpt::All {
            for (timestamp, logentries) in records {
                let a = aggregate_load(logentries, &filter_args.command);
                print_load(&fmt, sysconf, meta_args.verbose, logentries, *timestamp, &a);
            }
        } else  {
            // Invariant: there's always at least one record
            let (timestamp, ref logentries) = records[records.len()-1];
            let a = aggregate_load(logentries, &filter_args.command);
            print_load(&fmt, sysconf, meta_args.verbose, logentries, timestamp, &a);
        }
    }

    Ok(())
}

fn merge_sets(a: Option<HashSet<u32>>, b: &Option<HashSet<u32>>) -> Option<HashSet<u32>> {
    if a.is_none() && b.is_none() {
        return a;
    }
    let mut res = HashSet::new();
    if let Some(ref a) = a {
        for x in a {
            res.insert(*x);
        }
    }
    if let Some(ref b) = b {
        for x in b {
            res.insert(*x);
        }
    }
    Some(res)
}

fn aggregate_by_timeslot(
    bucket_opt: BucketOpt,
    command_filter: &Option<String>,
    records: &[(DateTime<Utc>, Vec<sonarlog::LogEntry>)]) -> Vec<(DateTime<Utc>, LoadAggregate)>
{
    // Create a vector `aggs` with the aggregate for the instant, and with a timestamp for
    // the instant rounded down to the start of the hour or day.  `aggs` will be sorted by
    // time, because `records` is.
    let mut aggs = records.iter()
        .map(|(t, x)| {
            let rounded_t = if bucket_opt == BucketOpt::Hourly {
                DateTime::from_utc(NaiveDate::from_ymd_opt(t.year(), t.month(), t.day())
                                   .unwrap()
                                   .and_hms_opt(t.hour(),0,0)
                                   .unwrap(),
                                   Utc)
            } else {
                DateTime::from_utc(NaiveDate::from_ymd_opt(t.year(), t.month(), t.day())
                                   .unwrap()
                                   .and_hms_opt(0,0,0)
                                   .unwrap(),
                                   Utc)
            };
            (rounded_t, aggregate_load(x, command_filter))
        })
        .collect::<Vec<(DateTime<Utc>, LoadAggregate)>>();

    // Bucket aggs by the rounded timestamps and re-sort in ascending time order.
    let mut by_timeslot = vec![];
    loop {
        if aggs.len() == 0 {
            break
        }
        let (t, agg) = aggs.pop().unwrap();
        let mut bucket = vec![agg];
        while aggs.len() > 0 && aggs.last().unwrap().0 == t {
            bucket.push(aggs.pop().unwrap().1);
        }
        by_timeslot.push((t, bucket));
    }
    by_timeslot.sort_by_key(|(timestamp, _)| timestamp.clone());

    // Compute averages.
    by_timeslot
        .iter()
        .map(|(timestamp, aggs)| {
            let n = aggs.len();
            (*timestamp, LoadAggregate {
                cpu_pct: aggs.iter().fold(0, |acc, a| acc + a.cpu_pct) / n,
                mem_gb: aggs.iter().fold(0, |acc, a| acc + a.mem_gb) / n,
                gpu_pct: aggs.iter().fold(0, |acc, a| acc + a.gpu_pct) / n,
                gpu_mem_pct: aggs.iter().fold(0, |acc, a| acc + a.gpu_mem_pct) / n,
                gpu_mem_gb: aggs.iter().fold(0, |acc, a| acc + a.gpu_mem_gb) / n,
                gpus: aggs.iter().fold(None, |acc, a| merge_sets(acc, &a.gpus)),
            })
        })
        .collect::<Vec<(DateTime<Utc>, LoadAggregate)>>()
}

fn aggregate_load(entries: &[sonarlog::LogEntry], command_filter: &Option<String>) -> LoadAggregate {
    let mut cpu_pct = 0.0;
    let mut mem_gb = 0.0;
    let mut gpu_pct = 0.0;
    let mut gpu_mem_pct = 0.0;
    let mut gpu_mem_gb = 0.0;
    let mut gpus : Option<HashSet<u32>> = None;
    for entry in entries {
        if let Some(s) = command_filter {
            if !entry.command.contains(s.as_str()) {
                continue
            }
        }
        cpu_pct += entry.cpu_pct;
        mem_gb += entry.mem_gb;
        gpu_pct += entry.gpu_pct;
        gpu_mem_pct += entry.gpu_mem_pct;
        gpu_mem_gb += entry.gpu_mem_gb;
        if entry.gpus.is_some() {
            gpus = merge_sets(gpus, &entry.gpus);
        }
    }
    LoadAggregate {
        cpu_pct: (cpu_pct * 100.0).ceil() as usize,
        mem_gb:  mem_gb.ceil() as usize,
        gpu_pct:  (gpu_pct * 100.0).ceil() as usize,
        gpu_mem_pct: (gpu_mem_pct * 100.0).ceil() as usize,
        gpu_mem_gb: gpu_mem_gb.ceil() as usize,
        gpus
    }
}

fn print_load(
    fmt: &[LoadFmt],
    config: Option<&configs::System>,
    verbose: bool,
    logentries: &[sonarlog::LogEntry],
    timestamp: DateTime<Utc>,
    a: &LoadAggregate)
{
    // The timestamp is either the time for the bucket (no aggregation) or the start of the hour or
    // day for aggregation.
    for x in fmt {
        // Problem: the field widths / maximal values here are true for individual buckets,
        // but not for aggregations across hosts.  (Aggregations across hosts have problems
        // in general, probably.)
        match x {
            LoadFmt::Date => {
                print!("{} ", timestamp.format("%Y-%m-%d "))
            }
            LoadFmt::Time => {
                print!("{} ", timestamp.format("%H:%M "))
            }
            LoadFmt::DateTime => {
                print!("{} ", timestamp.format("%Y-%m-%d %H:%M "))
            }
            LoadFmt::CpuPct => {
                print!("{:5} ", a.cpu_pct); // Max 99900
            }
            LoadFmt::RCpuPct => {
                let s = config.unwrap();
                print!("{:5}%", (((a.cpu_pct as f64) / (s.cpu_cores as f64 * 100.0)) * 100.0).round())
            }
            LoadFmt::MemGB => {
                print!("{:4} ", a.mem_gb)   // Max 9999
            }
            LoadFmt::RMemGB => {
                let s = config.unwrap();
                print!("{:5}%", (((a.mem_gb as f64) / (s.mem_gb as f64)) * 100.0).round())
            }
            LoadFmt::GpuPct => {
                print!("{:4} ", a.gpu_pct) // Max 6400
            }
            LoadFmt::RGpuPct => {
                let s = config.unwrap();
                print!("{:5}%", (((a.gpu_pct as f64) / (s.gpu_cards as f64 * 100.0)) * 100.0).round())
            }
            LoadFmt::VmemGB => {
                print!("{:4} ", a.gpu_mem_gb) // Max 9999
            }
            LoadFmt::RVmemGB => {
                let s = config.unwrap();
                print!("{:5}%", (((a.gpu_mem_gb as f64) / (s.gpu_mem_gb as f64)) * 100.0).round())
            }
            LoadFmt::VmemPct => {
                print!("{:4} ", a.gpu_mem_pct)   // Max 6400
            }
            LoadFmt::RVmemPct => {
                let s = config.unwrap();
                print!("{:5}%", (((a.gpu_mem_pct as f64) / (s.gpu_cards as f64 * 100.0)) * 100.0).round())
            }
            LoadFmt::GpuMask => {
                if a.gpus.is_some() {
                    // FIXME: don't print brackets.
                    // FIXME: distinguish none and unknown
                    let mut gpus = vec![];
                    for x in a.gpus.as_ref().unwrap() {
                        gpus.push(*x);
                    }
                    gpus.sort();
                    print!("{:?} ", gpus)
                } else {
                    print!("[]");
                }
            }
        }
    }
    if fmt.len() > 0 {
        println!("");
    }
    if verbose {
        for le in logentries {
            println!("   {} {} {} {} {} {:?} {} {}",
                     le.cpu_pct, le.mem_gb,
                     le.gpu_pct, le.gpu_mem_gb, le.gpu_mem_pct, le.gpus,
                     le.user, le.command)
        }
    }
}

fn compute_format(print_args: &LoadPrintArgs) -> Result<(Vec<LoadFmt>, bool)> {
    if let Some(ref fmt) = print_args.fmt {
        let mut v = vec![];
        let mut relative = false;
        for kwd in fmt.split(',') {
            match kwd {
                "date" => {
                    v.push(LoadFmt::Date)
                }
                "time" => {
                    v.push(LoadFmt::Time)
                }
                "datetime" => {
                    v.push(LoadFmt::DateTime)
                }
                "cpu" => {
                    v.push(LoadFmt::CpuPct)
                }
                "rcpu" => {
                    v.push(LoadFmt::RCpuPct);
                    relative = true
                }
                "mem" => {
                    v.push(LoadFmt::MemGB)
                }
                "rmem" => {
                    v.push(LoadFmt::RMemGB);
                    relative = true
                }
                "gpu" => {
                    v.push(LoadFmt::GpuPct)
                }
                "rgpu" => {
                    v.push(LoadFmt::RGpuPct);
                    relative = true;
                }
                "vmem" => {
                    v.push(LoadFmt::VmemGB);
                    v.push(LoadFmt::VmemPct)
                }
                "rvmem" => {
                    v.push(LoadFmt::RVmemGB);
                    v.push(LoadFmt::RVmemPct);
                    relative = true
                }
                "gpus" => {
                    v.push(LoadFmt::GpuMask)
                }
                _ => {
                    bail!("Bad load format keyword {kwd}")
                }
            }
        }
        Ok((v, relative))
    } else {
        Ok((vec![LoadFmt::DateTime,
                 LoadFmt::CpuPct,
                 LoadFmt::MemGB,
                 LoadFmt::GpuPct,
                 LoadFmt::VmemGB,
                 LoadFmt::VmemPct,
                 LoadFmt::GpuMask],
         false))
    }
}
