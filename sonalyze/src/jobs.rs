// Compute jobs aggregates from a set of log entries.

use crate::configs;
use crate::format;
use crate::{JobFilterArgs, JobPrintArgs, MetaArgs};

use anyhow::Result;
use sonarlog::{self, JobKey, LogEntry, Timestamp};
use std::boxed::Box;
use std::collections::{HashMap, HashSet};
use std::io;
use std::ops::Add;

#[cfg(all(feature = "untagged_sonar_data", test))]
use chrono::{Datelike, Timelike};
#[cfg(all(feature = "untagged_sonar_data", test))]
use std::io::Write;

pub fn aggregate_and_print_jobs(
    output: &mut dyn io::Write,
    system_config: &Option<HashMap<String, configs::System>>,
    filter_args: &JobFilterArgs,
    print_args: &JobPrintArgs,
    meta_args: &MetaArgs,
    joblog: HashMap<JobKey, Vec<Box<LogEntry>>>,
    earliest: Timestamp,
    latest: Timestamp,
) -> Result<()> {
    let mut jobvec =
        aggregate_and_filter_jobs(system_config, filter_args, joblog, earliest, latest);

    if meta_args.verbose {
        eprintln!(
            "Number of jobs after aggregation filtering: {}",
            jobvec.len()
        );
    }

    // And sort ascending by lowest beginning timestamp, and if those are equal (which happens when
    // we start reading logs at some arbitrary date), by job number.
    jobvec.sort_by(|a, b| {
        if a.0.first == b.0.first {
            a.1[0].job_id.cmp(&b.1[0].job_id)
        } else {
            a.0.first.cmp(&b.0.first)
        }
    });

    // Select a number of jobs per user, if applicable.  This means working from the bottom up
    // in the vector and marking the n first per user.  We need a hashmap user -> count.
    if let Some(n) = print_args.numjobs {
        let mut counts: HashMap<&str, usize> = HashMap::new();
        jobvec.iter_mut().rev().for_each(|(aggregate, job)| {
            if let Some(c) = counts.get(&(*job[0].user)) {
                if *c < n {
                    counts.insert(&job[0].user, *c + 1);
                } else {
                    aggregate.selected = false;
                }
            } else {
                counts.insert(&job[0].user, 1);
            }
        })
    }

    let numselected = jobvec
        .iter()
        .map(
            |(aggregate, _)| {
                if aggregate.selected {
                    1i32
                } else {
                    0i32
                }
            },
        )
        .reduce(i32::add)
        .unwrap_or(0);
    if meta_args.verbose {
        eprintln!("Number of jobs after output filtering: {}", numselected);
    }

    // Now print.

    if meta_args.verbose {
        return Ok(());
    }

    // Unix user names are max 8 chars.
    // Linux pids are max 7 decimal digits.
    // We don't care about seconds in the timestamp, nor timezone.

    if meta_args.raw {
        jobvec.iter().for_each(|(aggregate, job)| {
            output
                .write(
                    format!(
                        "{} job records\n\n{:?}\n\n{:?}\n",
                        job.len(),
                        &job[0..std::cmp::min(5, job.len())],
                        aggregate
                    )
                    .as_bytes(),
                )
                .unwrap();
        });
    } else if numselected > 0 {
        // TODO: For multi-host jobs, we probably want the option of printing many of these data
        // per-host and not summed across all hosts necessarily.  I can imagine a keyword that
        // controls this, `per-host` say.

        let mut formatters: HashMap<String, &dyn Fn(LogDatum, LogCtx) -> String> = HashMap::new();
        formatters.insert("jobm".to_string(), &format_jobm_id);
        formatters.insert("job".to_string(), &format_job_id);
        formatters.insert("user".to_string(), &format_user);
        formatters.insert("duration".to_string(), &format_duration);
        formatters.insert("start".to_string(), &format_start);
        formatters.insert("end".to_string(), &format_end);
        formatters.insert("cpu-avg".to_string(), &format_cpu_avg);
        formatters.insert("cpu-peak".to_string(), &format_cpu_peak);
        formatters.insert("mem-avg".to_string(), &format_mem_avg);
        formatters.insert("mem-peak".to_string(), &format_mem_peak);
        formatters.insert("gpu-avg".to_string(), &format_gpu_avg);
        formatters.insert("gpu-peak".to_string(), &format_gpu_peak);
        formatters.insert("gpumem-avg".to_string(), &format_gpumem_avg);
        formatters.insert("gpumem-peak".to_string(), &format_gpumem_peak);
        formatters.insert("cmd".to_string(), &format_command);
        formatters.insert("host".to_string(), &format_host);
        // TODO: More fields maybe:
        //
        //  rcpu - relative cpu% utilization, 100=all cores
        //  rmem - relative memory utilization, 100=all memory
        //  rgpu - relative gpu utilization, 100=all cards
        //  rgpumem - relative gpu memory utilization, 100=all memory on all cards
        //  gpus - list of gpus used, currently not part of aggregated data

        let spec = if let Some(ref fmt) = print_args.fmt {
            fmt
        } else {
            "jobm,user,duration,cpu-avg,cpu-peak,mem-avg,mem-peak,gpu-avg,gpu-peak,gpumem-avg,gpumem-peak,host,cmd"
        };
        let (fields, others) = format::parse_fields(spec, &formatters);
        let opts = format::standard_options(&others);
        if fields.len() > 0 {
            let selected = jobvec
                .drain(0..)
                .filter(|(aggregate, _)| aggregate.selected)
                .collect::<Vec<(JobAggregate, Vec<Box<LogEntry>>)>>();
            format::format_data(output, &fields, &formatters, &opts, selected, false);
        }
    }

    Ok(())
}

type LogDatum<'a> = &'a (JobAggregate, Vec<Box<LogEntry>>);
type LogCtx = bool; // Not used

fn format_user(datum: LogDatum, _: LogCtx) -> String {
    let (_, job) = datum;
    job[0].user.clone()
}

fn format_jobm_id(datum: LogDatum, _: LogCtx) -> String {
    let (aggregate, job) = datum;
    format!(
        "{}{}",
        job[0].job_id,
        if aggregate.classification & (LIVE_AT_START | LIVE_AT_END) == LIVE_AT_START | LIVE_AT_END {
            "!"
        } else if aggregate.classification & LIVE_AT_START != 0 {
            "<"
        } else if aggregate.classification & LIVE_AT_END != 0 {
            ">"
        } else {
            ""
        }
    )
}

fn format_job_id(datum: LogDatum, _: LogCtx) -> String {
    let (_, job) = datum;
    format!("{}", job[0].job_id)
}

fn format_host(datum: LogDatum, _: LogCtx) -> String {
    let (_, job) = datum;
    // At the moment, the hosts are in the jobs only
    let mut hosts = HashSet::new();
    for j in job {
        hosts.insert(j.hostname.split('.').next().unwrap().to_string());
    }
    let mut hostvec = hosts.iter().collect::<Vec<&String>>();
    if hostvec.len() == 1 {
        hostvec[0].clone()
    } else {
        hostvec.sort();
        let mut s = "".to_string();
        for h in hostvec {
            if !s.is_empty() {
                s += ",";
            }
            s += h;
        }
        s
    }
}

fn format_duration(datum: LogDatum, _: LogCtx) -> String {
    let (aggregate, _) = datum;
    format!(
        "{:}d{:2}h{:2}m",
        aggregate.days, aggregate.hours, aggregate.minutes
    )
}

fn format_start(datum: LogDatum, _: LogCtx) -> String {
    let (aggregate, _) = datum;
    aggregate.first.format("%Y-%m-%d %H:%M").to_string()
}

fn format_end(datum: LogDatum, _: LogCtx) -> String {
    let (aggregate, _) = datum;
    aggregate.last.format("%Y-%m-%d %H:%M").to_string()
}

fn format_cpu_avg(datum: LogDatum, _: LogCtx) -> String {
    let (aggregate, _) = datum;
    format!("{}", aggregate.cpu_avg)
}

fn format_cpu_peak(datum: LogDatum, _: LogCtx) -> String {
    let (aggregate, _) = datum;
    format!("{}", aggregate.cpu_peak)
}

fn format_mem_avg(datum: LogDatum, _: LogCtx) -> String {
    let (aggregate, _) = datum;
    format!("{}", aggregate.mem_avg)
}

fn format_mem_peak(datum: LogDatum, _: LogCtx) -> String {
    let (aggregate, _) = datum;
    format!("{}", aggregate.mem_peak)
}

fn format_gpu_avg(datum: LogDatum, _: LogCtx) -> String {
    let (aggregate, _) = datum;
    format!("{}", aggregate.gpu_avg)
}

fn format_gpu_peak(datum: LogDatum, _: LogCtx) -> String {
    let (aggregate, _) = datum;
    format!("{}", aggregate.gpu_peak)
}

fn format_gpumem_avg(datum: LogDatum, _: LogCtx) -> String {
    let (aggregate, _) = datum;
    format!("{}", aggregate.gpumem_avg)
}

fn format_gpumem_peak(datum: LogDatum, _: LogCtx) -> String {
    let (aggregate, _) = datum;
    format!("{}", aggregate.gpumem_peak)
}

fn format_command(datum: LogDatum, _: LogCtx) -> String {
    let (_, job) = datum;
    let mut names = HashSet::new();
    let mut name = "".to_string();
    for entry in job {
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

// TODO: Mildly worried about performance here.  We're computing a lot of attributes that we may or
// may not need and testing them even if they are not relevant.  But macro-effects may be more
// important anyway.  If we really care about efficiency we'll be interleaving aggregation and
// filtering so that we can bail out at the first moment the aggregated datum is not required.

// TODO: Aggregate the host names for the job into the JobAggregate, possibly under a flag since it
// involves building a hashmap and all of that.  It's possible the JobKey can carry the information
// sufficient for the flag.

fn aggregate_and_filter_jobs(
    system_config: &Option<HashMap<String, configs::System>>,
    filter_args: &JobFilterArgs,
    mut joblog: HashMap<JobKey, Vec<Box<LogEntry>>>,
    earliest: Timestamp,
    latest: Timestamp,
) -> Vec<(JobAggregate, Vec<Box<LogEntry>>)> {
    // Convert the aggregation filter options to a useful form.

    let min_cpu_avg = filter_args.min_cpu_avg as f64;
    let min_cpu_peak = filter_args.min_cpu_peak as f64;
    let max_cpu_avg = filter_args.max_cpu_avg as f64;
    let max_cpu_peak = filter_args.max_cpu_peak as f64;
    let min_rcpu_avg = filter_args.min_rcpu_avg as f64;
    let min_rcpu_peak = filter_args.min_rcpu_peak as f64;
    let max_rcpu_avg = filter_args.max_rcpu_avg as f64;
    let max_rcpu_peak = filter_args.max_rcpu_peak as f64;
    let min_mem_avg = filter_args.min_mem_avg;
    let min_mem_peak = filter_args.min_mem_peak;
    let min_rmem_avg = filter_args.min_rmem_avg as f64;
    let min_rmem_peak = filter_args.min_rmem_peak as f64;
    let min_gpu_avg = filter_args.min_gpu_avg as f64;
    let min_gpu_peak = filter_args.min_gpu_peak as f64;
    let max_gpu_avg = filter_args.max_gpu_avg as f64;
    let max_gpu_peak = filter_args.max_gpu_peak as f64;
    let min_rgpu_avg = filter_args.min_rgpu_avg as f64;
    let min_rgpu_peak = filter_args.min_rgpu_peak as f64;
    let max_rgpu_avg = filter_args.max_rgpu_avg as f64;
    let max_rgpu_peak = filter_args.max_rgpu_peak as f64;
    let min_samples = if let Some(n) = filter_args.min_samples {
        n
    } else {
        2
    };
    let min_runtime = if let Some(n) = filter_args.min_runtime {
        n.num_seconds()
    } else {
        0
    };
    let min_gpumem_avg = filter_args.min_gpumem_avg as f64;
    let min_gpumem_peak = filter_args.min_gpumem_peak as f64;
    let min_rgpumem_avg = filter_args.min_rgpumem_avg as f64;
    let min_rgpumem_peak = filter_args.min_rgpumem_peak as f64;

    // Get the vectors of jobs back into a vector, aggregate data, and filter the jobs.

    joblog
        .drain()
        .filter(|(_, job)| job.len() >= min_samples)
        .map(|(_, job)| (aggregate_job(system_config, &job, earliest, latest), job))
        .filter(|(aggregate, job)| {
            aggregate.cpu_avg >= min_cpu_avg
                && aggregate.cpu_peak >= min_cpu_peak
                && aggregate.cpu_avg <= max_cpu_avg
                && aggregate.cpu_peak <= max_cpu_peak
                && aggregate.mem_avg >= min_mem_avg as f64
                && aggregate.mem_peak >= min_mem_peak as f64
                && aggregate.gpu_avg >= min_gpu_avg
                && aggregate.gpu_peak >= min_gpu_peak
                && aggregate.gpu_avg <= max_gpu_avg
                && aggregate.gpu_peak <= max_gpu_peak
                && aggregate.gpumem_avg >= min_gpumem_avg
                && aggregate.gpumem_peak >= min_gpumem_peak
                && aggregate.duration >= min_runtime
                && (system_config.is_none()
                    || (aggregate.rcpu_avg >= min_rcpu_avg
                        && aggregate.rcpu_peak >= min_rcpu_peak
                        && aggregate.rcpu_avg <= max_rcpu_avg
                        && aggregate.rcpu_peak <= max_rcpu_peak
                        && aggregate.rmem_avg >= min_rmem_avg
                        && aggregate.rmem_peak >= min_rmem_peak
                        && aggregate.rgpu_avg >= min_rgpu_avg
                        && aggregate.rgpu_peak >= min_rgpu_peak
                        && aggregate.rgpu_avg <= max_rgpu_avg
                        && aggregate.rgpu_peak <= max_rgpu_peak
                        && aggregate.rgpumem_avg >= min_rgpumem_avg
                        && aggregate.rgpumem_peak >= min_rgpumem_peak))
                && {
                    if filter_args.no_gpu {
                        !aggregate.uses_gpu
                    } else {
                        true
                    }
                }
                && {
                    if filter_args.some_gpu {
                        aggregate.uses_gpu
                    } else {
                        true
                    }
                }
                && {
                    if filter_args.completed {
                        (aggregate.classification & LIVE_AT_END) == 0
                    } else {
                        true
                    }
                }
                && {
                    if filter_args.running {
                        (aggregate.classification & LIVE_AT_END) == 1
                    } else {
                        true
                    }
                }
                && {
                    if filter_args.zombie {
                        job[0].user.starts_with("_zombie_")
                    } else {
                        true
                    }
                }
                && {
                    if let Some(ref cmd) = filter_args.command {
                        job[0].command.contains(cmd)
                    } else {
                        true
                    }
                }
        })
        .collect::<Vec<(JobAggregate, Vec<Box<LogEntry>>)>>()
}

/// Bit values for JobAggregate::classification

const LIVE_AT_END: u32 = 1; // Earliest timestamp coincides with earliest record read
const LIVE_AT_START: u32 = 2; // Ditto latest/latest

// The JobAggregate structure holds aggregated data for a single job.  The view of the job may be
// partial, as job records may have been filtered out for the job for various reasons, including
// filtering by date range.
//
// Note the *_r* fields are only valid if there is a system_config present, otherwise they will be
// zero and should not be used.

#[derive(Debug)]
struct JobAggregate {
    first: Timestamp, // Earliest timestamp seen for job
    last: Timestamp,  // Latest ditto
    duration: i64,    // Duration in seconds
    minutes: i64,     // Duration as days:hours:minutes
    hours: i64,
    days: i64,

    uses_gpu: bool, // True if there's reason to believe a GPU was ever used by the job

    cpu_avg: f64,   // Average CPU utilization, 1 core == 100%
    cpu_peak: f64,  // Peak CPU utilization ditto
    rcpu_avg: f64,  // Average CPU utilization, all cores == 100%
    rcpu_peak: f64, // Peak CPU utilization ditto

    gpu_avg: f64,   // Average GPU utilization, 1 card == 100%
    gpu_peak: f64,  // Peak GPU utilization ditto
    rgpu_avg: f64,  // Average GPU utilization, all cards == 100%
    rgpu_peak: f64, // Peak GPU utilization ditto

    mem_avg: f64,   // Average main memory utilization, GiB
    mem_peak: f64,  // Peak memory utilization ditto
    rmem_avg: f64,  // Average main memory utilization, all memory = 100%
    rmem_peak: f64, // Peak memory utilization ditto

    // If a system config is present and conf.gpumem_pct is true then *_gpumem_gb are derived from
    // the recorded percentage figure, otherwise *_rgpumem are derived from the recorded absolute
    // figures.  If a system config is not present then all fields will represent the recorded
    // values (*_rgpumem the recorded percentages).
    gpumem_avg: f64,   // Average GPU memory utilization, GiB
    gpumem_peak: f64,  // Peak memory utilization ditto
    rgpumem_avg: f64,  // Average GPU memory utilization, all cards == 100%
    rgpumem_peak: f64, // Peak GPU memory utilization ditto

    selected: bool, // Initially true, it can be used to deselect the record before printing
    classification: u32, // Bitwise OR of flags above
}

// Given a list of log entries for a job, sorted ascending by timestamp, and the earliest and
// latest timestamps from all records read, return a JobAggregate for the job.
//
// TODO: Merge the folds into a single loop for efficiency?  Depends on what the compiler does.
//
// TODO: Are the ceil() calls desirable here or should they be applied during presentation?
//
// TODO: gpumem_pct is computed from a single host config, but in principle a job may span hosts
// and *really* in principle they could have cards that have a different value for that bit.  Don't
// know how to fix this.  It's a hack anyway.

fn aggregate_job(
    system_config: &Option<HashMap<String, configs::System>>,
    job: &[Box<LogEntry>],
    earliest: Timestamp,
    latest: Timestamp,
) -> JobAggregate {
    let first = job[0].timestamp;
    let last = job[job.len() - 1].timestamp;
    let host = &job[0].hostname;
    let duration = (last - first).num_seconds();
    let minutes = duration / 60;

    let uses_gpu = job.iter().any(|jr| jr.gpus.is_some());

    let cpu_avg = job.iter().fold(0.0, |acc, jr| acc + jr.cpu_util_pct) / (job.len() as f64);
    let cpu_peak = job.iter().fold(0.0, |acc, jr| f64::max(acc, jr.cpu_util_pct));
    let mut rcpu_avg = 0.0;
    let mut rcpu_peak = 0.0;

    let gpu_avg = job.iter().fold(0.0, |acc, jr| acc + jr.gpu_pct) / (job.len() as f64);
    let gpu_peak = job.iter().fold(0.0, |acc, jr| f64::max(acc, jr.gpu_pct));
    let mut rgpu_avg = 0.0;
    let mut rgpu_peak = 0.0;

    let mem_avg = job.iter().fold(0.0, |acc, jr| acc + jr.mem_gb) / (job.len() as f64);
    let mem_peak = job.iter().fold(0.0, |acc, jr| f64::max(acc, jr.mem_gb));
    let mut rmem_avg = 0.0;
    let mut rmem_peak = 0.0;

    let mut gpumem_avg = job.iter().fold(0.0, |acc, jr| acc + jr.gpumem_gb) / (job.len() as f64);
    let mut gpumem_peak = job.iter().fold(0.0, |acc, jr| f64::max(acc, jr.gpumem_gb));
    let gpumem_avg_pct = job.iter().fold(0.0, |acc, jr| acc + jr.gpumem_pct) / (job.len() as f64);
    let gpumem_peak_pct = job.iter().fold(0.0, |acc, jr| f64::max(acc, jr.gpumem_pct));
    let mut rgpumem_avg = gpumem_avg_pct;
    let mut rgpumem_peak = gpumem_peak_pct;

    if let Some(confs) = system_config {
        if let Some(conf) = confs.get(host) {
            let cpu_cores = conf.cpu_cores as f64;
            let mem = conf.mem_gb as f64;
            let gpu_cards = conf.gpu_cards as f64;
            let gpumem = conf.gpumem_gb as f64;

            rcpu_avg = cpu_avg / cpu_cores;
            rcpu_peak = cpu_peak / cpu_cores;

            rmem_avg = mem_avg / mem;
            rmem_peak = mem_peak / mem;

            rgpu_avg = gpu_avg / gpu_cards;
            rgpu_peak = gpu_peak / gpu_cards;

            if conf.gpumem_pct {
                gpumem_avg = (gpumem_avg_pct / 100.0) * gpumem;
                gpumem_peak = (gpumem_peak_pct / 100.0) * gpumem;
            } else {
                rgpumem_avg = gpumem_avg / gpumem;
                rgpumem_peak = gpumem_peak / gpumem;
            }
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
        duration,                   // total number of seconds
        minutes: minutes % 60,      // fractional hours
        hours: (minutes / 60) % 24, // fractional days
        days: minutes / (60 * 24),  // full days
        uses_gpu,
        cpu_avg: cpu_avg.ceil(),
        cpu_peak: cpu_peak.ceil(),
        rcpu_avg: rcpu_avg.ceil(),
        rcpu_peak: rcpu_peak.ceil(),
        gpu_avg: gpu_avg.ceil(),
        gpu_peak: gpu_peak.ceil(),
        rgpu_avg: rgpu_avg.ceil(),
        rgpu_peak: rgpu_peak.ceil(),
        mem_avg: mem_avg.ceil(),
        mem_peak: mem_peak.ceil(),
        rmem_avg: rmem_avg.ceil(),
        rmem_peak: rmem_peak.ceil(),
        gpumem_avg: gpumem_avg.ceil(),
        gpumem_peak: gpumem_peak.ceil(),
        rgpumem_avg: rgpumem_avg.ceil(),
        rgpumem_peak: rgpumem_peak.ceil(),
        selected: true,
        classification,
    }
}

#[cfg(feature = "untagged_sonar_data")]
#[test]
fn test_compute_jobs3() {
    // job 2447150 crosses files

    // Filter by job ID, we just want the one job
    let filter = |e:&LogEntry| e.job_id == 2447150;
    let (jobs, _numrec, earliest, latest) = sonarlog::compute_jobs(
        &vec![
            "../sonar_test_data0/2023/05/31/ml8.hpc.uio.no.csv".to_string(),
            "../sonar_test_data0/2023/06/01/ml8.hpc.uio.no.csv".to_string(),
        ],
        &filter,
        /* merge_across_hosts= */ false,
    )
    .unwrap();

    assert!(jobs.len() == 1);
    let job = jobs
        .get(&JobKey::from_parts(
            /* by_host= */ true,
            "ml8.hpc.uio.no",
            2447150,
        ))
        .unwrap();

    // First record
    // 2023-06-23T12:25:01.486240376+00:00,ml8.hpc.uio.no,192,larsbent,2447150,python,173,18813976,1000,0,0,833536
    //
    // Last record
    // 2023-06-24T09:00:01.386294752+00:00,ml8.hpc.uio.no,192,larsbent,2447150,python,161,13077760,1000,0,0,833536

    let start = job[0].timestamp;
    let end = job[job.len() - 1].timestamp;
    assert!(
        start.year() == 2023
            && start.month() == 6
            && start.day() == 23
            && start.hour() == 12
            && start.minute() == 25
            && start.second() == 1
    );
    assert!(
        end.year() == 2023
            && end.month() == 6
            && end.day() == 24
            && end.hour() == 9
            && end.minute() == 0
            && end.second() == 1
    );

    let agg = aggregate_job(&None, job, earliest, latest);
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

// Presumably there's something standard for this
#[cfg(all(feature = "untagged_sonar_data", test))]
struct Collector {
    storage: Vec<u8>,
}

#[cfg(all(feature = "untagged_sonar_data", test))]
impl Collector {
    fn new() -> Collector {
        Collector { storage: vec![] }
    }

    fn get(&mut self) -> String {
        String::from_utf8(self.storage.clone()).unwrap()
    }
}

#[cfg(all(feature = "untagged_sonar_data", test))]
impl io::Write for Collector {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.storage.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "untagged_sonar_data")]
#[test]
fn test_format_jobs() {
    let filter = |e:&LogEntry| e.job_id <= 2447150;
    let (jobs, _numrec, earliest, latest) = sonarlog::compute_jobs(
        &vec![
            "../sonar_test_data0/2023/05/31/ml8.hpc.uio.no.csv".to_string(),
            "../sonar_test_data0/2023/06/01/ml8.hpc.uio.no.csv".to_string(),
        ],
        &filter,
        /* merge_across_hosts= */ false,
    )
    .unwrap();

    let mut filter_args = JobFilterArgs::default();
    // TODO: Annoying and does not scale - surely there's a better way?
    filter_args.max_cpu_avg = 100000000;
    filter_args.max_cpu_peak = 100000000;
    filter_args.max_rcpu_avg = 100000000;
    filter_args.max_rcpu_peak = 100000000;
    filter_args.max_gpu_avg = 100000000;
    filter_args.max_gpu_peak = 100000000;
    filter_args.max_rgpu_avg = 100000000;
    filter_args.max_rgpu_peak = 100000000;
    let print_args = JobPrintArgs::default();
    let meta_args = MetaArgs::default();
    let mut c = Collector::new();
    aggregate_and_print_jobs(
        &mut c,
        &None,
        &filter_args,
        &print_args,
        &meta_args,
        jobs,
        earliest,
        latest,
    )
    .unwrap();
    c.flush().unwrap();
    let contents = c.get();
    let expected =
"jobm      user      duration  cpu-avg  cpu-peak  mem-avg  mem-peak  gpu-avg  gpu-peak  gpumem-avg  gpumem-peak  host  cmd            
4079<     root      1d16h55m  4        4         1        1         0        0         0           0            ml8   tuned          
4093!     zabbix    1d17h 0m  5        5         1        1         0        0         0           0            ml8   zabbix_agentd  
585616<   larsbent  0d 0h45m  933      1273      194      199       72       84        16          26           ml8   python         
1649588<  riccarsi  0d 3h20m  141      141       127      155       38       44        2           2            ml8   python         
2381069<  einarvid  1d16h55m  2        2         4        4         0        0         0           0            ml8   mongod         
1592463   larsbent  0d 2h44m  594      1292      92       116       76       89        20          37           ml8   python         
1593746   larsbent  0d 2h44m  2701     2834      21       29        52       71        2           3            ml8   python         
1921146   riccarsi  0d20h50m  143      146       104      115       38       42        2           2            ml8   python         
1939269   larsbent  0d 2h59m  536      3095      116      132       79       92        19          33           ml8   python         
1940843   larsbent  0d 2h59m  260      888       47       62        46       58        2           3            ml8   python         
2126454   riccarsi  0d 6h44m  131      134       149      149       57       59        2           3            ml8   python         
2447150   larsbent  0d20h34m  163      178       18       19        0        0         1           1            ml8   python         
";
    assert!(expected == contents);
}
