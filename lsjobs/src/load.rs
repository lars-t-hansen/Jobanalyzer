use chrono::prelude::{DateTime,NaiveDate};
use chrono::{Datelike,Timelike,Utc};
use sonarlog;
use crate::{Cli,configs};

// Fields that can be printed for `--load`.
//
// Note that GPU memory is tricky.  On NVidia, the "percentage" is unreliable.  On AMD, the absolute
// value is unobtainable (on our current systems).

enum LoadFmt {
    Date,                       // YYYY-MM-DD
    Time,                       // HH:SS
    DateTime,                   // YYYY-MM-DD HH:SS
    CpuPct,                     // Accumulated CPU percentage, 100==1 core
    RCpuPct,
    MemGB,                      // Accumulated memory usage, GB
    RMemGB,
    GpuPct,                     // Accumulated GPU percentage, 100==1 card
    RGpuPct,
    VmemGB,                     // Accumulated GPU memory usage, GB
    RVmemGB,
    VmemPct,                    // Accumulated GPU memory usage percentage, 100==1 card
    RVmemPct,
    GpuMask                     // Accumulated GPUs in use
}

// We read and filter sonar records, bucket by host, sort by ascending timestamp, and then
// bucket by timestamp.  The buckets can then be aggregated into a "load" value for each time.

// TODO:
//
// - Really `last` and `hourly` (say) can be combined...  But do we care?
//
// - A complication is that all of these numbers are also relative to an absolute max (eg a 128-core
//   system has max 12800% CPU) and often we're more interested in the load of the system relative
//   to its configuration.
//
//   I think that there could perhaps be a --config=filename switch that loads the configuration of
//   hosts.  (There could be a default.)  There could be a --relative switch that requires that file
//   to be read and used.
//
// - For some listings it may be desirable to print a heading?

pub fn aggregate_and_print_load(
    cli: &Cli,
    by_host: &[(String, Vec<(DateTime<Utc>, Vec<sonarlog::LogEntry>)>)],
    which_listing: &str)
{
    let (fmt, relative) = compute_format(cli);

    let config = if relative {
        if cli.config_file.is_none() {
            // error - FIXME
            eprintln!("ERROR: relative values requested without config file");
            return;
        }
        let config_filename = cli.config_file.as_ref().unwrap();
        let config_result = configs::read_from_json(&config_filename);
        if let Err(e) = config_result {
            // error - FIXME
            eprintln!("ERROR: relative values requested but config file not read: {}", e);
            return;
        }
        Some(config_result.unwrap())
    } else {
        None
    };

    // by_host is sorted ascending by hostname (outer string) and time (inner timestamp)

    for (hostname, records) in by_host {
        // We always print host name unless there's only one and it was selected explicitly.
        if by_host.len() != 1 || cli.host.is_none() {
            println!("HOST: {}", hostname);
        }
        let sysconf = if let Some(ref ht) = config {
            ht.get(hostname)
        } else {
            None
        };
            
        if which_listing == "hourly" || which_listing == "daily" {
            // Create a vector `aggs` with the aggregate for the instant, and with a timestamp for
            // the instant rounded down to the start of the hour or day.  `aggs` will be sorted by
            // time, because `records` is.
            let mut aggs = records.iter()
                .map(|(t, x)| {
                    let rounded_t = if which_listing == "hourly" {
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
                    (rounded_t, sonarlog::aggregate_load(x))
                })
                .collect::<Vec<(DateTime<Utc>, sonarlog::LoadAggregate)>>();

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

            // Compute averages and print them.
            for (timestamp, aggs) in by_timeslot {
                let n = aggs.len();
                let avg = sonarlog::LoadAggregate {
                    cpu_pct: aggs.iter().fold(0, |acc, a| acc + a.cpu_pct) / n,
                    mem_gb: aggs.iter().fold(0, |acc, a| acc + a.mem_gb) / n,
                    gpu_pct: aggs.iter().fold(0, |acc, a| acc + a.gpu_pct) / n,
                    gpu_mem_pct: aggs.iter().fold(0, |acc, a| acc + a.gpu_mem_pct) / n,
                    gpu_mem_gb: aggs.iter().fold(0, |acc, a| acc + a.gpu_mem_gb) / n,
                    gpu_mask: aggs.iter().fold(0, |acc, a| acc | a.gpu_mask)
                };
                print_load(&fmt, sysconf, cli.verbose, &vec![], timestamp, &avg);
            }
        }
        else if which_listing == "all" {
            for (timestamp, logentries) in records {
                let a = sonarlog::aggregate_load(logentries);
                print_load(&fmt, sysconf, cli.verbose, logentries, *timestamp, &a);
            }
        } else if which_listing == "last" {
            // Invariant: there's always at least one record
            let (timestamp, ref logentries) = records[records.len()-1];
            let a = sonarlog::aggregate_load(logentries);
            print_load(&fmt, sysconf, cli.verbose, logentries, timestamp, &a);
        } else {
            panic!("Unrecognized spec for --load")
        }
    }
}

// If config is not none then we also want relative-to-system values

fn print_load(fmt: &[LoadFmt], config: Option<&configs::System>, verbose: bool, logentries: &[sonarlog::LogEntry], timestamp: DateTime<Utc>, a: &sonarlog::LoadAggregate) {
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
                print!("{:b} ", a.gpu_mask)      // Max 2^64-1
            }
        }
    }
    println!("");
    if verbose {
        for le in logentries {
            println!("   {} {} {} {} {} {} {} {}",
                     le.cpu_pct, le.mem_gb,
                     le.gpu_pct, le.gpu_mem_gb, le.gpu_mem_pct, le.gpu_mask,
                     le.user, le.command)
        }
    }
}

fn compute_format(cli: &Cli) -> (Vec<LoadFmt>, bool) {
    if let Some(ref fmt) = cli.loadfmt {
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
                _ => { /* What to do? */ }
            }
        }
        (v, relative)
    } else {
        (vec![LoadFmt::DateTime,LoadFmt::CpuPct,LoadFmt::MemGB,LoadFmt::GpuPct,LoadFmt::VmemGB,LoadFmt::VmemPct,LoadFmt::GpuMask],
         false)
    }
}
