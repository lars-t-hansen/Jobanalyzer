// Compute jobs aggregates from a set of log entries.

use crate::configs;
use crate::{JobFilterArgs, JobPrintArgs, MetaArgs};

use anyhow::Result;
#[cfg(test)]
use chrono::{Datelike,Timelike};
use sonarlog::{self, LogEntry, Timestamp};
use std::collections::{HashMap, HashSet};
use std::ops::Add;

pub fn aggregate_and_print_jobs(
    system_config: &Option<HashMap<String, configs::System>>,
    filter_args: &JobFilterArgs,
    print_args: &JobPrintArgs,
    meta_args: &MetaArgs,
    mut joblog: HashMap::<u32, Vec<LogEntry>>,
    earliest: Timestamp,
    latest: Timestamp) -> Result<()>
{
    // Convert the aggregation filter options to a useful form.

    let min_avg_cpu = filter_args.min_avg_cpu as f64;
    let min_peak_cpu = filter_args.min_peak_cpu as f64;
    let min_avg_rcpu = filter_args.min_avg_rcpu as f64;
    let min_peak_rcpu = filter_args.min_peak_rcpu as f64;
    let max_avg_rcpu = filter_args.max_avg_rcpu as f64;
    let max_peak_rcpu = filter_args.max_peak_rcpu as f64;
    let min_avg_mem = filter_args.min_avg_mem;
    let min_peak_mem = filter_args.min_peak_mem;
    let min_avg_rmem = filter_args.min_avg_rmem as f64;
    let min_peak_rmem = filter_args.min_peak_rmem as f64;
    let min_avg_gpu = filter_args.min_avg_gpu as f64;
    let min_peak_gpu = filter_args.min_peak_gpu as f64;
    let min_avg_rgpu = filter_args.min_avg_rgpu as f64;
    let min_peak_rgpu = filter_args.min_peak_rgpu as f64;
    let max_avg_rgpu = filter_args.min_avg_rgpu as f64;
    let max_peak_rgpu = filter_args.min_peak_rgpu as f64;
    let min_samples = if let Some(n) = filter_args.min_samples { n } else { 2 };
    let min_runtime = if let Some(n) = filter_args.min_runtime { n.num_seconds() } else { 0 };
    let min_avg_vmem = filter_args.min_avg_vmem as f64;
    let min_peak_vmem = filter_args.min_peak_vmem as f64;
    let min_avg_rvmem = filter_args.min_avg_rvmem as f64;
    let min_peak_rvmem = filter_args.min_peak_rvmem as f64;

    // Get the vectors of jobs back into a vector, aggregate data, and filter the jobs.

    let mut jobvec = joblog
        .drain()
        .filter(|(_, job)| job.len() >= min_samples)
        .map(|(_, job)| (aggregate_job(system_config, &job, earliest, latest), job))
        .filter(|(aggregate, job)| {
            aggregate.avg_cpu >= min_avg_cpu &&
                aggregate.peak_cpu >= min_peak_cpu &&
                aggregate.avg_mem_gb >= min_avg_mem as f64 &&
                aggregate.peak_mem_gb >= min_peak_mem as f64 &&
                aggregate.avg_gpu >= min_avg_gpu &&
                aggregate.peak_gpu >= min_peak_gpu &&
                aggregate.avg_vmem_pct >= min_avg_vmem &&
                aggregate.peak_vmem_pct >= min_peak_vmem &&
                aggregate.duration >= min_runtime &&
                (system_config.is_none() ||
                 (aggregate.avg_rcpu >= min_avg_rcpu &&
                  aggregate.peak_rcpu >= min_peak_rcpu &&
                  aggregate.avg_rcpu <= max_avg_rcpu &&
                  aggregate.peak_rcpu <= max_peak_rcpu &&
                  aggregate.avg_rmem >= min_avg_rmem &&
                  aggregate.peak_rmem >= min_peak_rmem &&
                  aggregate.avg_rgpu >= min_avg_rgpu &&
                  aggregate.peak_rgpu >= min_peak_rgpu &&
                  aggregate.avg_rgpu <= max_avg_rgpu &&
                  aggregate.peak_rgpu <= max_peak_rgpu)) &&
            { if filter_args.no_gpu { !aggregate.uses_gpu } else { true } } &&
            { if filter_args.some_gpu { aggregate.uses_gpu } else { true } } &&
            { if filter_args.completed { (aggregate.classification & LIVE_AT_END) == 0 } else { true } } &&
            { if filter_args.running { (aggregate.classification & LIVE_AT_END) == 1 } else { true } } &&
            { if filter_args.zombie { job[0].user.starts_with("_zombie_") } else { true } } &&
            { if let Some(ref cmd) = filter_args.command { job[0].command.contains(cmd) } else { true } }
        })
        .collect::<Vec<(JobAggregate, Vec<LogEntry>)>>();

    if meta_args.verbose {
        eprintln!("Number of jobs after aggregation filtering: {}", jobvec.len());
    }

    // And sort ascending by lowest beginning timestamp
    jobvec.sort_by(|a, b| a.0.first.cmp(&b.0.first));

    // Select a number of jobs per user, if applicable.  This means working from the bottom up
    // in the vector and marking the n first per user.  We need a hashmap user -> count.
    if let Some(n) = print_args.numjobs {
        let mut counts: HashMap<&str,usize> = HashMap::new();
        jobvec.iter_mut().rev().for_each(|(aggregate, job)| {
            if let Some(c) = counts.get(&(*job[0].user)) {
                if *c < n {
                    counts.insert(&job[0].user, *c+1);
                } else {
                    aggregate.selected = false;
                }
            } else {
                counts.insert(&job[0].user, 1);
            }
        })
    }

    if meta_args.verbose {
        let numselected = jobvec.iter()
            .map(|(aggregate, _)| {
                if aggregate.selected { 1i32 } else { 0i32 }
            })
            .reduce(i32::add)
            .unwrap_or(0);
        eprintln!("Number of jobs after output filtering: {}", numselected);
    }

    // Now print.
    //
    // Unix user names are max 8 chars.
    // Linux pids are max 7 decimal digits.
    // We don't care about seconds in the timestamp, nor timezone.

    if meta_args.raw {
        jobvec.iter().for_each(|(aggregate, job)| {
            println!("{} job records\n\n{:?}\n\n{:?}\n", job.len(), &job[0..std::cmp::min(5,job.len())], aggregate);
        });
    } else {
        println!("{:8} {:8}   {:9}   {:16}   {:16}   {:9}  {:9}  {:9}  {:9}   {}",
                 "job#", "user", "time", "start?", "end?", "cpu", "mem gb", "gpu", "gpu mem", "command", );
        let tfmt = "%Y-%m-%d %H:%M";
        jobvec.iter().for_each(|(aggregate, job)| {
            if aggregate.selected {
                let dur = format!("{:2}d{:2}h{:2}m", aggregate.days, aggregate.hours, aggregate.minutes);
                println!("{:7}{} {:8}   {}   {}   {}   {:4}/{:4}  {:4}/{:4}  {:4}/{:4}  {:4}/{:4}   {}",
                         job[0].job_id,
                         if aggregate.classification & (LIVE_AT_START|LIVE_AT_END) == LIVE_AT_START|LIVE_AT_END {
                             "!"
                         } else if aggregate.classification & LIVE_AT_START != 0 {
                             "<"
                         } else if aggregate.classification & LIVE_AT_END != 0 {
                             ">"
                         } else {
                             " "
                         },
                         job[0].user,
                         dur,
                         aggregate.first.format(tfmt),
                         aggregate.last.format(tfmt),
                         aggregate.avg_cpu,
                         aggregate.peak_cpu,
                         aggregate.avg_mem_gb,
                         aggregate.peak_mem_gb,
                         aggregate.avg_gpu,
                         aggregate.peak_gpu,
                         aggregate.avg_vmem_pct,
                         aggregate.peak_vmem_pct,
                         job_name(job));
            }
        });
    }

    Ok(())
}

fn job_name(entries: &[LogEntry]) -> String {
    let mut names = HashSet::new();
    let mut name = "".to_string();
    for entry in entries {
        if names.contains(&entry.command) {
            continue;
        }
        if name != "" {
            name += ", ";
        }
        name += &entry.command;
        names.insert(&entry.command);
    }
    name
}

/// Bit values for JobAggregate::classification

const LIVE_AT_END : u32 = 1;   // Earliest timestamp coincides with earliest record read
const LIVE_AT_START : u32 = 2; // Ditto latest/latest

/// The JobAggregate structure holds aggregated data for a single job.  The view of the job may be
/// partial, as job records may have been filtered out for the job for various reasons, including
/// filtering by date range.
///
/// TODO: Document weirdness around GPU memory utilization.
/// TODO: Why not absolute GPU memory utilization also?

#[derive(Debug)]
struct JobAggregate {
    first: Timestamp,       // Earliest timestamp seen for job
    last: Timestamp,        // Latest ditto
    duration: i64,          // Duration in seconds
    minutes: i64,           // Duration as days:hours:minutes
    hours: i64,
    days: i64,
    uses_gpu: bool,         // True if there's reason to believe a GPU was ever used by the job
    avg_cpu: f64,           // Average CPU utilization, 1 core == 100%
    peak_cpu: f64,          // Peak CPU utilization ditto
    avg_rcpu: f64,          // Average CPU utilization, all cores == 100%
    peak_rcpu: f64,         // Peak CPU utilization ditto
    avg_gpu: f64,           // Average GPU utilization, 1 card == 100%
    peak_gpu: f64,          // Peak GPU utilization ditto
    avg_rgpu: f64,          // Average GPU utilization, all cards == 100%
    peak_rgpu: f64,         // Peak GPU utilization ditto
    avg_mem_gb: f64,        // Average main memory utilization, GiB
    peak_mem_gb: f64,       // Peak memory utilization ditto
    avg_rmem: f64,          // Average main memory utilization, all memory = 100%
    peak_rmem: f64,         // Peak memory utilization ditto
    avg_vmem_pct: f64,      // Average GPU memory utilization, 1 card == 100%
    peak_vmem_pct: f64,     // Peak GPU memory utilization ditto
    selected: bool,         // Initially true, it can be used to deselect the record before printing
    classification: u32,    // Bitwise OR of flags above
}

/// Given a list of log entries for a job, sorted ascending by timestamp, and the earliest and
/// latest timestamps from all records read, return a JobAggregate for the job.

fn aggregate_job(
    system_config: &Option<HashMap<String, configs::System>>,
    job: &[LogEntry],
    earliest: Timestamp,
    latest: Timestamp) -> JobAggregate
{
    let first = job[0].timestamp;
    let last = job[job.len()-1].timestamp;
    let host = &job[0].hostname;
    let duration = (last - first).num_seconds();
    let minutes = duration / 60;
    let uses_gpu = job.iter().any(|jr| jr.gpus.is_some());
    let avg_cpu = job.iter().fold(0.0, |acc, jr| acc + jr.cpu_pct) / (job.len() as f64);
    let peak_cpu = job.iter().fold(0.0, |acc, jr| f64::max(acc, jr.cpu_pct));
    let avg_gpu = job.iter().fold(0.0, |acc, jr| acc + jr.gpu_pct) / (job.len() as f64);
    let peak_gpu = job.iter().fold(0.0, |acc, jr| f64::max(acc, jr.gpu_pct));
    let avg_mem_gb = job.iter().fold(0.0, |acc, jr| acc + jr.mem_gb) / (job.len() as f64);
    let peak_mem_gb = job.iter().fold(0.0, |acc, jr| f64::max(acc, jr.mem_gb));
    let avg_vmem_pct = job.iter().fold(0.0, |acc, jr| acc + jr.gpu_mem_pct) /  (job.len() as f64);
    let peak_vmem_pct = job.iter().fold(0.0, |acc, jr| f64::max(acc, jr.gpu_mem_pct));
    let mut avg_rcpu = 0.0;
    let mut peak_rcpu = 0.0;
    let mut avg_rgpu = 0.0;
    let mut peak_rgpu = 0.0;
    let mut avg_rmem = 0.0;
    let mut peak_rmem = 0.0;
    if let Some(confs) = system_config {
        if let Some(conf) = confs.get(host) {
            let cpu_cores = conf.cpu_cores as f64;
            let gpu_cards = conf.gpu_cards as f64;
            avg_rcpu = avg_cpu / cpu_cores;
            peak_rcpu = peak_cpu / cpu_cores;
            avg_rgpu = avg_gpu / gpu_cards;
            peak_rgpu = peak_gpu / gpu_cards;
            avg_rmem = avg_mem_gb / conf.mem_gb as f64;
            peak_rmem = peak_mem_gb / conf.mem_gb as f64;
        }
    }

    let mut classification = 0;
    if first == earliest {
        classification |= LIVE_AT_START;
    }
    if last == latest {
        classification |= LIVE_AT_END;
    }
    JobAggregate {
        first,
        last,
        duration,                               // total number of seconds
        minutes: minutes % 60,                  // fractional hours
        hours: (minutes / 60) % 24,             // fractional days
        days: minutes / (60 * 24),              // full days
        uses_gpu,
        avg_cpu: avg_cpu.ceil(),
        peak_cpu: peak_cpu.ceil(),
        avg_rcpu: avg_rcpu.ceil(),
        peak_rcpu: peak_rcpu.ceil(),
        avg_gpu: avg_gpu.ceil(),
        peak_gpu: peak_gpu.ceil(),
        avg_rgpu: avg_rgpu.ceil(),
        peak_rgpu: peak_rgpu.ceil(),
        avg_mem_gb: avg_mem_gb.ceil(),
        peak_mem_gb: peak_mem_gb.ceil(),
        avg_rmem: avg_rmem.ceil(),
        peak_rmem: peak_rmem.ceil(),
        avg_vmem_pct: avg_vmem_pct.ceil(),
        peak_vmem_pct: peak_vmem_pct.ceil(),
        selected: true,
        classification,
    }
}

#[test]
fn test_compute_jobs3() {
    // job 2447150 crosses files

    // Filter by job ID, we just want the one job
    let filter = |_user:&str, _host:&str, job: u32, _t:&Timestamp| {
        job == 2447150
    };
    let (jobs, _numrec, earliest, latest) = sonarlog::compute_jobs(&vec![
        "../sonar_test_data0/2023/05/31/ml8.hpc.uio.no.csv".to_string(),
        "../sonar_test_data0/2023/06/01/ml8.hpc.uio.no.csv".to_string()],
                         &filter).unwrap();

    assert!(jobs.len() == 1);
    let job = jobs.get(&2447150).unwrap();

    // First record
    // 2023-06-23T12:25:01.486240376+00:00,ml8.hpc.uio.no,192,larsbent,2447150,python,173,18813976,1000,0,0,833536
    //
    // Last record
    // 2023-06-24T09:00:01.386294752+00:00,ml8.hpc.uio.no,192,larsbent,2447150,python,161,13077760,1000,0,0,833536

    let start = job[0].timestamp;
    let end = job[job.len()-1].timestamp;
    assert!(start.year() == 2023 && start.month() == 6 && start.day() == 23 &&
            start.hour() == 12 && start.minute() == 25 && start.second() == 1);
    assert!(end.year() == 2023 && end.month() == 6 && end.day() == 24 &&
            end.hour() == 9 && end.minute() == 0 && end.second() == 1);

    let agg = aggregate_job(job, earliest, latest);
    assert!(agg.classification == 0);
    assert!(agg.first == start);
    assert!(agg.last == end);
    assert!(agg.duration == (end - start).num_seconds());
    assert!(agg.days == 0);
    assert!(agg.hours == 20);
    assert!(agg.minutes == 34);
    assert!(agg.uses_gpu);
    assert!(agg.selected);
    // TODO: Really more here
}
