/// Compute jobs aggregates from a set of log entries.

use crate::prjobs;
use crate::{JobFilterAndAggregationArgs, JobPrintArgs, MetaArgs};

use anyhow::{bail, Result};
use sonarlog::{self, LogEntry, InputStreamSet, Timestamp};
use std::boxed::Box;
use std::collections::HashMap;
use std::io;

#[cfg(all(feature = "untagged_sonar_data", test))]
use chrono::{Datelike, Timelike};
#[cfg(all(feature = "untagged_sonar_data", test))]
use std::io::Write;

/// Bit values for JobAggregate::classification

pub const LIVE_AT_END: u32 = 1;   // Earliest timestamp coincides with earliest record read
pub const LIVE_AT_START: u32 = 2; // Ditto latest/latest
// Subjob level is eight bits at this offset
pub const LEVEL_SHIFT: u32 = 8;
pub const LEVEL_MASK: u32 = 255;

/// The JobAggregate structure holds aggregated data for a single job.  The view of the job may be
/// partial, as job records may have been filtered out for the job for various reasons, including
/// filtering by date range.
///
/// Note the *_r* fields are only valid if there is a system_config present, otherwise they will be
/// zero and should not be used.

#[derive(Debug)]
pub struct JobAggregate {
    pub first: Timestamp, // Earliest timestamp seen for job
    pub last: Timestamp,  // Latest ditto
    pub duration: i64,    // Duration in seconds
    pub minutes: i64,     // Duration as days:hours:minutes
    pub hours: i64,
    pub days: i64,

    pub uses_gpu: bool, // True if there's reason to believe a GPU was ever used by the job

    pub cpu_avg: f64,   // Average CPU utilization, 1 core == 100%
    pub cpu_peak: f64,  // Peak CPU utilization ditto
    pub rcpu_avg: f64,  // Average CPU utilization, all cores == 100%
    pub rcpu_peak: f64, // Peak CPU utilization ditto

    pub gpu_avg: f64,   // Average GPU utilization, 1 card == 100%
    pub gpu_peak: f64,  // Peak GPU utilization ditto
    pub rgpu_avg: f64,  // Average GPU utilization, all cards == 100%
    pub rgpu_peak: f64, // Peak GPU utilization ditto

    pub mem_avg: f64,   // Average main memory utilization, GiB
    pub mem_peak: f64,  // Peak memory utilization ditto
    pub rmem_avg: f64,  // Average main memory utilization, all memory = 100%
    pub rmem_peak: f64, // Peak memory utilization ditto

    // If a system config is present and conf.gpumem_pct is true then gpumem_* are derived from the
    // recorded percentage figure, otherwise rgpumem_* are derived from the recorded absolute
    // figures.  If a system config is not present then all fields will represent the recorded
    // values (rgpumem_* the recorded percentages).
    pub gpumem_avg: f64,   // Average GPU memory utilization, GiB
    pub gpumem_peak: f64,  // Peak memory utilization ditto
    pub rgpumem_avg: f64,  // Average GPU memory utilization, all cards == 100%
    pub rgpumem_peak: f64, // Peak GPU memory utilization ditto

    pub selected: bool, // Initially true, it can be used to deselect the record before printing
    pub classification: u32, // Bitwise OR of flags above
}

// Convenient package for results from aggregation.

pub struct JobSummary {
    pub job: Vec<Box<LogEntry>>,            // The records going into this job
    pub aggregate: JobAggregate,            // Aggregate of those records
    pub breakdown: Option<(String, Vec<JobSummary>)>, // Components of the job, if requested
}

pub fn aggregate_and_print_jobs(
    output: &mut dyn io::Write,
    system_config: &Option<HashMap<String, sonarlog::System>>,
    filter_args: &JobFilterAndAggregationArgs,
    print_args: &JobPrintArgs,
    meta_args: &MetaArgs,
    streams: InputStreamSet,
    earliest: Timestamp,
    latest: Timestamp,
) -> Result<()> {
    let orig_streams = if print_args.breakdown.is_some() {
        Some(streams.clone())
    } else {
        None
    };

    let mut jobvec =
        aggregate_and_filter_jobs(system_config, filter_args, streams, earliest, latest);

    if meta_args.verbose {
        eprintln!(
            "Number of jobs after aggregation filtering: {}",
            jobvec.len()
        );
    }

    if let Some(ref breakdown) = print_args.breakdown {
        let kwds = breakdown.split(",").collect::<Vec<&str>>();
        let mut host = 0;
        let mut command = 0;
        let mut other = 0;
        for k in kwds.iter() {
            match k {
                &"host" => { host += 1; }
                &"command" => { command += 1; }
                _ => { other += 1; }
            }
        }
        if host > 1 || command > 1 || other > 0 {
            bail!("Bad breakdown spec {breakdown}");
        }
        attach_breakdown(system_config,
                         filter_args,
                         &kwds,
                         0,
                         &mut jobvec,
                         orig_streams.unwrap(),
                         earliest,
                         latest);
    }

    prjobs::print_jobs(output, system_config, jobvec, print_args, meta_args)
}

// Assume breakdown by "X"
// First partition input streams by job (across hosts and commands)
// Then partition those streams by Xs (across Ys)
// Then aggregate the data in the latter partitions
// Then attach these aggregates to the job

fn attach_breakdown(
    system_config: &Option<HashMap<String, sonarlog::System>>,
    filter_args: &JobFilterAndAggregationArgs,
    kwds: &[&str],
    kwdix: usize,
    jobvec: &mut Vec<JobSummary>,
    mut orig_streams: InputStreamSet,
    earliest: Timestamp,
    latest: Timestamp,
) {
    // If the streams in `orig_streams` are unique enough to be in a HashMap then the subset
    // we're creating by job here - the value in this hashmap - will also have unique keys if
    // they reuse the InputStreamKey.

    let mut streams_by_job: HashMap<u32, InputStreamSet> = HashMap::new();
    for (key, streams) in orig_streams.drain() {
        let job_id = streams[0].job_id;
        if job_id == 0 {
            continue;
        }
        if let Some(iss) = streams_by_job.get_mut(&job_id) {
            iss.insert(key, streams);
        } else {
            let mut hm = HashMap::new();
            hm.insert(key, streams);
            streams_by_job.insert(job_id, hm);
        }
    }

    for (job_id, job_streams) in streams_by_job.drain() {
        // TODO: This lookup is going to be quadratic
        let job = jobvec.iter_mut().find(|j| j.job[0].job_id == job_id).unwrap();
        attach_one_breakdown(system_config, filter_args, kwds, kwdix, job, job_streams, earliest, latest);
    }
}

fn attach_one_breakdown(
    system_config: &Option<HashMap<String, sonarlog::System>>,
    filter_args: &JobFilterAndAggregationArgs,
    kwds: &[&str],
    kwdix: usize,
    job: &mut JobSummary,
    mut job_streams: InputStreamSet,
    earliest: Timestamp,
    latest: Timestamp,
) {
    let kwd = kwds[kwdix];

    // Partition the streams for the job by host or command.  Again, it's fine to reuse the
    // InputStreamKey as the key for the inner map.

    let mut streams_by_x: HashMap<String, InputStreamSet> = HashMap::new();
    for (key, streams) in job_streams.drain() {
        let k = if kwd == "host" { key.0.clone() } else { key.2.clone() };
        if let Some(iss) = streams_by_x.get_mut(&k) {
            iss.insert(key, streams);
        } else {
            let mut hm = HashMap::new();
            hm.insert(key.clone(), streams);
            streams_by_x.insert(k, hm);
        }
    }

    // Aggregate the accumulated streams, and if necessary, descend another level to break down
    // further.

    let mut breakdown = vec![];
    for (_x, x_streams) in streams_by_x {
        let next_streams = if kwdix < kwds.len()-1 { Some(x_streams.clone()) } else { None };
        let mut summaries =
            aggregate_and_filter_jobs(system_config, filter_args, x_streams, earliest, latest);
        assert!(summaries.len() == 1);
        let mut summary = summaries.pop().unwrap();
        if let Some(orig_x_streams) = next_streams {
            attach_one_breakdown(system_config, filter_args, kwds, kwdix+1, &mut summary, orig_x_streams, earliest, latest);
        }
        breakdown.push(summary);
    }
    let tag = kwd.to_string();
    job.breakdown = Some((tag, breakdown));
}

// A sample stream is a quadruple (host, command, job-related-id, record-list).  A stream is only
// ever about one job.  There may be multiple streams per job, they will all have the same
// job-related-id which is unique but not necessarily equal to any field in any of the records.
//
// This function collects the data per job and returns a vector of (aggregate, records) pairs where
// the aggregate describes the job in aggregate and the records is a synthesized stream of sample
// records for the job, based on all the input streams for the job.  The manner of the synthesis
// depends on arguments to the program: with --batch we merge across hosts, otherwise not.
//
// TODO: Mildly worried about performance here.  We're computing a lot of attributes that we may or
// may not need and testing them even if they are not relevant.  But macro-effects may be more
// important anyway.  If we really care about efficiency we'll be interleaving aggregation and
// filtering so that we can bail out at the first moment the aggregated datum is not required.

fn aggregate_and_filter_jobs(
    system_config: &Option<HashMap<String, sonarlog::System>>,
    filter_args: &JobFilterAndAggregationArgs,
    streams: InputStreamSet,
    earliest: Timestamp,
    latest: Timestamp,
) -> Vec<JobSummary> {
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

    let aggregate_filter =
        |JobSummary { aggregate, job, .. } : &JobSummary| {
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
		        job.iter().any(|x| x.command.contains("<defunct>") || x.user.starts_with("_zombie_"))
                    } else {
                        true
                    }
                }
        };

    // Select streams and synthesize a merged stream, and then aggregate and print it.

    let mut jobs = 
        if filter_args.batch {
            sonarlog::merge_by_job(streams)
        } else {
            sonarlog::merge_by_host_and_job(streams)
        };
    jobs
        .drain(0..)
        .filter(|job| job.len() >= min_samples)
        .map(|job| JobSummary {
            aggregate: aggregate_job(system_config, &job, earliest, latest),
            job,
            breakdown: None,
        })
        .filter(&aggregate_filter)
        .collect::<Vec<JobSummary>>()
}

// Given a list of log entries for a job, sorted ascending by timestamp and with no duplicated
// timestamps, and the earliest and latest timestamps from all records read, return a JobAggregate
// for the job.
//
// TODO: Merge the folds into a single loop for efficiency?  Depends on what the compiler does.
//
// TODO: Are the ceil() calls desirable here or should they be applied during presentation?

fn aggregate_job(
    system_config: &Option<HashMap<String, sonarlog::System>>,
    job: &[Box<LogEntry>],
    earliest: Timestamp,
    latest: Timestamp,
) -> JobAggregate {
    let first = job[0].timestamp;
    let last = job[job.len() - 1].timestamp;
    let host = &job[0].hostname;
    let duration = (last - first).num_seconds();
    let minutes = ((duration as f64) / 60.0).round() as i64;

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

    let gpumem_avg = job.iter().fold(0.0, |acc, jr| acc + jr.gpumem_gb) / (job.len() as f64);
    let gpumem_peak = job.iter().fold(0.0, |acc, jr| f64::max(acc, jr.gpumem_gb));
    let mut rgpumem_avg = 0.0;
    let mut rgpumem_peak = 0.0;
    
    if let Some(confs) = system_config {
        if let Some(conf) = confs.get(host) {
            let cpu_cores = conf.cpu_cores as f64;
            let mem = conf.mem_gb as f64;
            let gpu_cards = conf.gpu_cards as f64;
            let gpumem = conf.gpumem_gb as f64;

            rcpu_avg = cpu_avg / cpu_cores;
            rcpu_peak = cpu_peak / cpu_cores;

            rmem_avg = (mem_avg * 100.0) / mem;
            rmem_peak = (mem_peak * 100.0) / mem;

            rgpu_avg = gpu_avg / gpu_cards;
            rgpu_peak = gpu_peak / gpu_cards;

            // If we have a config then logclean will have computed proper GPU memory values for the
            // job, so we need not look to conf.gpumem_pct here.  If we don't have a config then we
            // don't care about these figures anyway.
            rgpumem_avg = gpumem_avg / gpumem;
            rgpumem_peak = gpumem_peak / gpumem;
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
fn test_compute_jobs() {
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
