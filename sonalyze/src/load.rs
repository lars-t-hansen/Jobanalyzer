// Compute system load aggregates from a set of log entries.

// TODO:
//
// - For some listings it may be desirable to print a heading?

use crate::configs;
use crate::format;
use crate::{LoadFilterArgs, LoadPrintArgs, MetaArgs};

use anyhow::{bail,Result};
use sonarlog::{self, HostFilter, Timestamp};
use std::collections::{HashMap, HashSet};
use std::io;

#[derive(Clone, Debug)]
struct LoadAggregate {
    cpu_pct: usize,
    mem_gb: usize,
    gpu_pct: usize,
    gpumem_pct: usize,
    gpumem_gb: usize,
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
    system_config: &Option<HashMap<String, configs::System>>,
    _include_hosts: &HostFilter,
    filter_args: &LoadFilterArgs,
    print_args: &LoadPrintArgs,
    meta_args: &MetaArgs,
    by_host: &[(String, Vec<(Timestamp, Vec<sonarlog::LogEntry>)>)]) -> Result<()>
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

    let mut formatters : HashMap<String, &dyn Fn(LoadDatum, LoadCtx) -> String> = HashMap::new();
    formatters.insert("date".to_string(), &format_date);
    formatters.insert("time".to_string(), &format_time);
    formatters.insert("cpu".to_string(), &format_cpu);
    formatters.insert("rcpu".to_string(), &format_rcpu);
    formatters.insert("mem".to_string(), &format_mem);
    formatters.insert("rmem".to_string(), &format_rmem);
    formatters.insert("gpu".to_string(), &format_gpu);
    formatters.insert("rgpu".to_string(), &format_rgpu);
    formatters.insert("gpumem".to_string(), &format_gpumem);
    formatters.insert("rgpumem".to_string(), &format_rgpumem);
    formatters.insert("gpus".to_string(), &format_gpus);

    let spec = if let Some(ref fmt) = print_args.fmt {
        fmt
    } else {
        "date,time,cpu,mem,gpu,gpumem,gpumask"
    };
    let (fields, others) = format::parse_fields(spec, &formatters);
    let csv = others.get("csv").is_some();
    let header = (!csv && !others.get("noheader").is_some()) || (csv && others.get("header").is_some());
    let relative = fields.iter().any(|x| {
        match *x {
            "rcpu" | "rmem" | "rgpu" | "rgpumem" => true,
            _ => false
        }
    });

    if relative && system_config.is_none() {
        bail!("Relative values requested without config file");
    }

    // Now print.

    if meta_args.verbose {
        return Ok(())
    }

    // by_host is sorted ascending by hostname (outer string) and time (inner timestamp)

    for (hostname, records) in by_host {
        // We always print host name unless there's only one and it was selected explicitly.
        // FIXME: This is ill-defined with the better host filtering.  Unless an FQDN was added
        // with the "exhaustive" flag this should never be true.
        /*
        if include_hosts.len() != 1 {
            println!("HOST: {}", hostname);
        }
        */

        println!("HOST: {}", hostname);

        let sysconf = if let Some(ref ht) = system_config {
            ht.get(hostname)
        } else {
            None
        };
            
        if bucket_opt != BucketOpt::None {
            let by_timeslot = aggregate_by_timeslot(bucket_opt, &filter_args.command, records);
            if print_opt == PrintOpt::All {
                format::format_data(&mut io::stdout(), &fields, &formatters, header, csv, by_timeslot, &sysconf);
            } else {
                let (timestamp, avg) = by_timeslot[by_timeslot.len()-1].clone();
                let data = vec![(timestamp, avg)];
                format::format_data(&mut io::stdout(), &fields, &formatters, header, csv, data, &sysconf);
            }
        } else if print_opt == PrintOpt::All {
            let data = records.iter().map(|(timestamp, logentries)| {
                (*timestamp, aggregate_load(logentries, &filter_args.command))
            }).collect::<Vec<(Timestamp, LoadAggregate)>>();
            format::format_data(&mut io::stdout(), &fields, &formatters, header, csv, data, &sysconf);
        } else {
            // Invariant: there's always at least one record
            let (timestamp, ref logentries) = records[records.len()-1];
            let a = aggregate_load(logentries, &filter_args.command);
            let data = vec![(timestamp, a)];
            format::format_data(&mut io::stdout(), &fields, &formatters, header, csv, data, &sysconf);
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
    records: &[(Timestamp, Vec<sonarlog::LogEntry>)]) -> Vec<(Timestamp, LoadAggregate)>
{
    // Create a vector `aggs` with the aggregate for the instant, and with a timestamp for
    // the instant rounded down to the start of the hour or day.  `aggs` will be sorted by
    // time, because `records` is.
    let mut aggs = records.iter()
        .map(|(t, x)| {
            let rounded_t = if bucket_opt == BucketOpt::Hourly {
                sonarlog::truncate_to_hour(*t)
            } else {
                sonarlog::truncate_to_day(*t)
            };
            (rounded_t, aggregate_load(x, command_filter))
        })
        .collect::<Vec<(Timestamp, LoadAggregate)>>();

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
                gpumem_pct: aggs.iter().fold(0, |acc, a| acc + a.gpumem_pct) / n,
                gpumem_gb: aggs.iter().fold(0, |acc, a| acc + a.gpumem_gb) / n,
                gpus: aggs.iter().fold(None, |acc, a| merge_sets(acc, &a.gpus)),
            })
        })
        .collect::<Vec<(Timestamp, LoadAggregate)>>()
}

fn aggregate_load(entries: &[sonarlog::LogEntry], command_filter: &Option<String>) -> LoadAggregate {
    let mut cpu_pct = 0.0;
    let mut mem_gb = 0.0;
    let mut gpu_pct = 0.0;
    let mut gpumem_pct = 0.0;
    let mut gpumem_gb = 0.0;
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
        gpumem_pct += entry.gpumem_pct;
        gpumem_gb += entry.gpumem_gb;
        if entry.gpus.is_some() {
            gpus = merge_sets(gpus, &entry.gpus);
        }
    }
    LoadAggregate {
        cpu_pct: cpu_pct.ceil() as usize,
        mem_gb:  mem_gb.ceil() as usize,
        gpu_pct:  gpu_pct.ceil() as usize,
        gpumem_pct: gpumem_pct.ceil() as usize,
        gpumem_gb: gpumem_gb.ceil() as usize,
        gpus
    }
}

type LoadDatum<'a> = &'a (Timestamp, LoadAggregate);
type LoadCtx<'a> = &'a Option<&'a configs::System>;

fn format_date((t, _): LoadDatum, _: LoadCtx) -> String {
    t.format("%Y-%m-%d").to_string()
}

fn format_time((t, _): LoadDatum, _: LoadCtx) -> String {
    t.format("%H:%M").to_string()
}

fn format_cpu((_, a): LoadDatum, _: LoadCtx) -> String {
    format!("{}", a.cpu_pct)
}

fn format_rcpu((_, a): LoadDatum, config: LoadCtx) -> String {
    let s = config.unwrap();
    format!("{}", ((a.cpu_pct as f64) / (s.cpu_cores as f64)).round())
}

fn format_mem((_, a): LoadDatum, _: LoadCtx) -> String {
    format!("{}", a.mem_gb)
}

fn format_rmem((_, a): LoadDatum, config: LoadCtx) -> String {
    let s = config.unwrap();
    format!("{}", ((a.mem_gb as f64) / (s.mem_gb as f64)).round())
}

fn format_gpu((_, a): LoadDatum, _: LoadCtx) -> String {
    format!("{}", a.gpu_pct)
}

fn format_rgpu((_, a): LoadDatum, config: LoadCtx) -> String {
    let s = config.unwrap();
    format!("{}", ((a.gpu_pct as f64) / (s.gpu_cards as f64)).round())
}

fn format_gpumem((_, a): LoadDatum, _: LoadCtx) -> String {
    format!("{}", a.gpumem_gb)
}

fn format_rgpumem((_, a): LoadDatum, config: LoadCtx) -> String {
    let s = config.unwrap();
    format!("{}", ((a.gpumem_gb as f64) / (s.gpumem_gb as f64)).round())
}

fn format_gpus((_, a): LoadDatum, _: LoadCtx) -> String {
    if let Some(ref gpus) = a.gpus {
        if gpus.is_empty() {
            "none".to_string()
        } else {
            let mut gpunums = vec![];
            for x in gpus {
                gpunums.push(*x);
            }
            gpunums.sort();
            let mut s = "".to_string();
            for x in gpunums {
                if !s.is_empty() {
                    s += ",";
                }
                s += &format!("{}", x)
            }
            s
        }
    } else {
        "unknown".to_string()
    }
}
