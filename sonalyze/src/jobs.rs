use anyhow::Result;
use chrono::prelude::DateTime;
use chrono::Utc;
use sonarlog;
use std::collections::HashMap;
use std::ops::Add;
use crate::{JobFilterArgs,JobPrintArgs,MetaArgs};

pub fn aggregate_and_print_jobs(
    maybe_command: &Option<String>,
    filter_args: &JobFilterArgs,
    print_args: &JobPrintArgs,
    meta_args: &MetaArgs,
    mut joblog: HashMap::<u32, Vec<sonarlog::LogEntry>>,
    earliest: DateTime<Utc>,
    latest: DateTime<Utc>) -> Result<()>
{
    // Convert the aggregation filter options to a useful form.

    let min_avg_cpu = filter_args.min_avg_cpu as f64;
    let min_peak_cpu = filter_args.min_peak_cpu as f64;
    let min_avg_mem = filter_args.min_avg_mem;
    let min_peak_mem = filter_args.min_peak_mem;
    let min_avg_gpu = filter_args.min_avg_gpu as f64;
    let min_peak_gpu = filter_args.min_peak_gpu as f64;
    let min_observations = if let Some(n) = filter_args.min_observations { n } else { 2 };
    let min_runtime = if let Some(n) = filter_args.min_runtime { n.num_seconds() } else { 0 };
    let min_avg_vmem = filter_args.min_avg_vmem as f64;
    let min_peak_vmem = filter_args.min_peak_vmem as f64;

    // Get the vectors of jobs back into a vector, aggregate data, and filter the jobs.

    let mut jobvec = joblog
        .drain()
        .filter(|(_, job)| job.len() >= min_observations)
        .map(|(_, job)| (sonarlog::aggregate_job(&job, earliest, latest), job))
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
            { if filter_args.no_gpu { !aggregate.uses_gpu } else { true } } &&
            { if filter_args.some_gpu { aggregate.uses_gpu } else { true } } &&
            { if filter_args.completed { (aggregate.classification & sonarlog::LIVE_AT_END) == 0 } else { true } } &&
            { if filter_args.running { (aggregate.classification & sonarlog::LIVE_AT_END) == 1 } else { true } } &&
            { if filter_args.zombie { job[0].user.starts_with("_zombie_") } else { true } } &&
            { if let Some(ref cmd) = maybe_command { job[0].command.contains(cmd) } else { true } }
        })
        .collect::<Vec<(sonarlog::JobAggregate, Vec<sonarlog::LogEntry>)>>();

    if meta_args.verbose {
        eprintln!("Number of job records after aggregation filtering: {}", jobvec.len());
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
        eprintln!("Number of job records after output filtering: {}", numselected);
    }

    // Now print.
    //
    // Unix user names are max 8 chars.
    // Linux pids are max 7 decimal digits.
    // We don't care about seconds in the timestamp, nor timezone.

    if meta_args.raw {
        jobvec.iter().for_each(|(aggregate, job)| {
            println!("{:?}\n{:?}\n", job[0], aggregate);
        });
    } else {
        println!("{:8} {:8}   {:9}   {:16}   {:16}   {:9}  {:9}  {:9}  {:9}   {}",
                 "job#", "user", "time", "start?", "end?", "cpu", "mem gb", "gpu", "gpu mem", "command", );
        let tfmt = "%Y-%m-%d %H:%M";
        jobvec.iter().for_each(|(aggregate, job)| {
            if aggregate.selected {
                let dur = format!("{:2}d{:2}h{:2}m", aggregate.days, aggregate.hours, aggregate.minutes);
                println!("{:7}{} {:8}   {}   {}   {}   {:4}/{:4}  {:4}/{:4}  {:4}/{:4}  {:4}/{:4}   {:22}",
                         job[0].job_id,
                         if aggregate.classification & (sonarlog::LIVE_AT_START|sonarlog::LIVE_AT_END) == sonarlog::LIVE_AT_START|sonarlog::LIVE_AT_END {
                             "!"
                         } else if aggregate.classification & sonarlog::LIVE_AT_START != 0 {
                             "<"
                         } else if aggregate.classification & sonarlog::LIVE_AT_END != 0 {
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
                         job[0].command);
            }
        });
    }

    Ok(())
}

