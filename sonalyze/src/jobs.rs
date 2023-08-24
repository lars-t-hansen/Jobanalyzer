/// Compute jobs aggregates from a set of log entries.

use crate::configs;
use crate::prjobs;
use crate::{JobFilterAndAggregationArgs, JobPrintArgs, MetaArgs};

use anyhow::Result;
use sonarlog::{self, empty_gpuset, union_gpuset, JobKey, LogEntry, Timestamp};
use std::boxed::Box;
use std::collections::{HashMap, HashSet};
use std::io;

#[cfg(all(feature = "untagged_sonar_data", test))]
use chrono::{Datelike, Timelike};
#[cfg(all(feature = "untagged_sonar_data", test))]
use std::io::Write;

/// Bit values for JobAggregate::classification

pub const LIVE_AT_END: u32 = 1; // Earliest timestamp coincides with earliest record read
pub const LIVE_AT_START: u32 = 2; // Ditto latest/latest

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

    // If a system config is present and conf.gpumem_pct is true then *_gpumem_gb are derived from
    // the recorded percentage figure, otherwise *_rgpumem are derived from the recorded absolute
    // figures.  If a system config is not present then all fields will represent the recorded
    // values (*_rgpumem the recorded percentages).
    pub gpumem_avg: f64,   // Average GPU memory utilization, GiB
    pub gpumem_peak: f64,  // Peak memory utilization ditto
    pub rgpumem_avg: f64,  // Average GPU memory utilization, all cards == 100%
    pub rgpumem_peak: f64, // Peak GPU memory utilization ditto

    pub selected: bool, // Initially true, it can be used to deselect the record before printing
    pub classification: u32, // Bitwise OR of flags above
}

pub fn aggregate_and_print_jobs(
    output: &mut dyn io::Write,
    system_config: &Option<HashMap<String, configs::System>>,
    filter_args: &JobFilterAndAggregationArgs,
    print_args: &JobPrintArgs,
    meta_args: &MetaArgs,
    joblog: HashMap<JobKey, Vec<Box<LogEntry>>>,
    earliest: Timestamp,
    latest: Timestamp,
) -> Result<()> {
    let jobvec =
        aggregate_and_filter_jobs(system_config, filter_args, joblog, earliest, latest);

    if meta_args.verbose {
        eprintln!(
            "Number of jobs after aggregation filtering: {}",
            jobvec.len()
        );
    }

    prjobs::print_jobs(output, system_config, jobvec, print_args, meta_args)
}

// TODO: Mildly worried about performance here.  We're computing a lot of attributes that we may or
// may not need and testing them even if they are not relevant.  But macro-effects may be more
// important anyway.  If we really care about efficiency we'll be interleaving aggregation and
// filtering so that we can bail out at the first moment the aggregated datum is not required.

fn aggregate_and_filter_jobs(
    system_config: &Option<HashMap<String, configs::System>>,
    filter_args: &JobFilterAndAggregationArgs,
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

    let aggregate_filter =
        |(aggregate, job) : &(JobAggregate, Vec<Box<LogEntry>>)| {
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
                && {
                    if let Some(ref cmd) = filter_args.command {
                        job[0].command.contains(cmd)
                    } else {
                        true
                    }
                }
        };

    // Get the vectors of jobs back into a vector, aggregate data, and filter the jobs.

    if filter_args.batch {
        synthesize_batched_jobs(joblog)
            .drain(0..)
            .filter(|(_, job)| job.len() >= min_samples)
            .map(|(_, job)| (aggregate_job(system_config, &job, earliest, latest), job))
            .filter(&aggregate_filter)
            .collect::<Vec<(JobAggregate, Vec<Box<LogEntry>>)>>()
    } else {
        joblog
            .drain()
            .filter(|(_, job)| job.len() >= min_samples)
            .map(|(_, job)| (aggregate_job(system_config, &job, earliest, latest), job))
            .filter(&aggregate_filter)
            .collect::<Vec<(JobAggregate, Vec<Box<LogEntry>>)>>()
    }
}

// What does batching of multi-host jobs mean?
//
// Consider peak cpu.  The normal interpretation of this is the highest valued sample for CPU
// utilization across the run.  For aggregate, we can't simply sum the values of peak CPU because
// those peaks did not necessarily happen around the same time.  Samples will not in general have
// been taken at the same time.
//
// But consider all event streams from all hosts in the job in parallel, here "+" denotes a sample
// and "-" denotes time just passing, we have three cores, and each character is one time tick:
//
//   t= 01234567890123456789
//   C1 --+---+---
//   C2 -+----+---
//   C3 ---+----+-
//
// At t=1, we get a reading for C2.  This value is now in effect until t=6 when we have a new
// sample for C2.  For C1, we have readings at t=2 and t=6.  We wish to "reconstruct" a CPU
// utilization sample across C1, C2, and C3.  An obvious way to do it is to create samples at t=1,
// t=2, t=3, t=6, t=8.  The values that we create for the sample at eg t=3 are the values in effect
// for C1 and C2 from earlier and the new value for C3 at t=3.  The total CPU utilization at that
// time is the sum of the three values, and that goes into computing the peak.
//
// Thus in some sense, batching means creating an event stream that captures these values from the
// raw LogEntries, and then processing the new stream.  The "LogEntries" that we create will have
// aggregate host sets (effectively just an aggregate host name that is the same value in every
// record) and gpu sets (just a union).
//
// The resulting Vec<Box<LogEntry>> is just that - a vector of the synthesized job entries.
//
//  given vector V of event streams for a set of hosts and a common job ID:
//  given vector A of "current observed values for all streams", initially "0"
//  while some streams in V are not empty
//     get lowest time  (*) (**) across nonempty streams of V
//     update A with values from the those streams
//     advance those streams
//     push out a new event record with current values
//
// Then do our normal aggregation with the generated list of event records.
//
// (*) There may be multiple record with the lowest time, and we should do all of them at the same
//     time, to reduce the volume of output.
//
// (**) In practice, sonar will be run by cron and cron is pretty good about running jobs when
//      they're supposed to run.  Therefore there will be a fair amount of correlation across hosts
//      about when these samples are gathered, ie, records will cluster around points in time.  We
//      should capture these clusters by considering all records that are within a W-second window
//      after the earliest next record to have the same time.  In practice W will be small (on the
//      order of 5, I'm guessing).  The time for the synthesized record could be the time of the
//      earliest record, or a midpoint or other statistical quantity of the times that go into the
//      record.
//
// synthesize_batched_jobs() returns vector of (job_id, synthesized-records-for-job) where the
// synthesized records for a single job all have the following artifacts, and job_id is unique in
// the vector.  Let R be the records that went into synthesizing a single record according to the
// algorithm above and S be all the input records for the job.  Then:
//
//   - version is the highest version found in R
//   - timestamp is synthesized from the timestamps of R
//   - hostname is the same in all records and is derived from the host names of S
//   - num_cores is 0
//   - user is the user name of any record in S - they should all be the same
//   - pid is 0
//   - job_id is the job ID of any record in S - they should all be the same
//   - command is the same in all records and is derived from the commands of S
//   - cpu_pct is the sum across the cpu_pct of R
//   - mem_gb is the sum across the mem_gb of R
//   - gpus is the union of the gpus across R
//   - gpu_pct is the sum across the gpu_pct of R
//   - gpumem_pct is the sum across the gpumem_pct of R
//   - gpumem_gb is the sum across the gpumem_gb of R
//   - cputime_sec is the sum across the cputime_sec of R
//   - rolledup is the number of records in the list
//   - cpu_util_pct is the sum across the cpu_util_pct of R (roughly the best we can do)

fn synthesize_batched_jobs(mut joblog: HashMap<JobKey, Vec<Box<LogEntry>>>) -> Vec<(u32, Vec<Box<LogEntry>>)> {
    
    // Collect the event streams for the job across all hosts.  Each stream is Vec<Box<LogEntry>>.
    //
    // The key in this map is the job ID.
    //
    // The value in this map has the set of hostnames, the set of commands, and the vectors of
    // records for the job ID across all hosts (the event streams).

    let mut newlog : HashMap<u32, (HashSet<String>, HashSet<String>, Vec<Vec<Box<LogEntry>>>)> = HashMap::new();
    for ((hostname, job_id), stream) in joblog.drain() {
        if let Some((hostnames, commands, streams)) = newlog.get_mut(&job_id) {
            hostnames.insert(hostname);
            for r in &stream {
                commands.insert(r.command.clone());
            }
            streams.push(stream);
        } else {
            let mut hostnames = HashSet::new();
            hostnames.insert(hostname);
            let mut commands = HashSet::new();
            for r in &stream {
                commands.insert(r.command.clone());
            }
            let streams = vec![stream];
            newlog.insert(job_id, (hostnames, commands, streams));
        }
    }

    // Vector of (job_id, records)
    let mut results = vec![];

    for (job_id, (mut hosts, mut commands, streams)) in newlog.drain() {
        // TODO: Host names should be sorted and collapsed before joining.
        let hostname = hosts.drain().collect::<Vec<String>>().join(",");
        // TODO: For commands, we would probably be happiest maintaining a hit count and sorting by
        // decreasing hit count before joining.
        let command = commands.drain().collect::<Vec<String>>().join(",");
        // Any user from any record is fine.  There should be an invariant that no stream is empty,
        // so this should always be safe.
        let user = streams[0][0].user.clone();

        // Generated records
        let mut records = vec![];

        // indices[i] has the index of the next element of stream[i]
        let mut indices = [0].repeat(streams.len());
        loop {
            // Loop across streams to find smallest head.
            // smallest_stream is -1 or the index of the stream with the smallest head
            let mut smallest_stream = 0;
            let mut have_smallest = false;
            for i in 0..streams.len() {
                if indices[i] >= streams[i].len() {
                    continue;
                }
                // stream[i] has a value, select this stream if we have no stream or if the value is
                // smaller than the one at the head of the smallest stream.
                if !have_smallest || streams[smallest_stream][indices[smallest_stream]].timestamp > streams[i][indices[i]].timestamp {
                    smallest_stream = i;
                    have_smallest = true;
                }
            }

            // Exit if no values in any stream
            if !have_smallest {
                break;
            }

            let min_time = streams[smallest_stream][indices[smallest_stream]].timestamp;
            let lim_time = min_time + chrono::Duration::seconds(10);

            // Now select all values from all streams that fit within the time window, and advance
            // the stream pointers for those streams.
            let mut selected : Vec<&Box<LogEntry>> = vec![];
            for i in 0..streams.len() {
                if indices[i] >= streams[i].len() {
                    continue;
                }
                if streams[i][indices[i]].timestamp < lim_time {
                    selected.push(&streams[i][indices[i]]);
                    indices[i] += 1;
                }
            }

            // Now compute 
            let version = "0".to_string();  // FIXME, really "version max" across selected records
            let cpu_pct = selected.iter().fold(0.0, |acc, x| acc + x.cpu_pct);
            let mem_gb = selected.iter().fold(0.0, |acc, x| acc + x.mem_gb);
            let gpu_pct = selected.iter().fold(0.0, |acc, x| acc + x.gpu_pct);
            let gpumem_pct = selected.iter().fold(0.0, |acc, x| acc + x.gpumem_pct);
            let gpumem_gb = selected.iter().fold(0.0, |acc, x| acc + x.gpumem_gb);
            let cputime_sec = selected.iter().fold(0.0, |acc, x| acc + x.cputime_sec);
            let cpu_util_pct = selected.iter().fold(0.0, |acc, x| acc + x.cpu_util_pct);
            // The invariant here is that rolledup is the number of *other* processes rolled up into
            // this one.  So we add one for each in the list + the others rolled into each of those,
            // and subtract one at the end to maintain the invariant.
            let rolledup = selected.iter().fold(0, |acc, x| acc + x.rolledup + 1) - 1;
            let mut gpus = empty_gpuset();
            for s in selected {
                union_gpuset(&mut gpus, &s.gpus);
            }

            // Now generate a datum
            records.push(Box::new(LogEntry {
                version,
                timestamp: min_time,
                hostname: hostname.clone(),
                num_cores: 0,
                user: user.clone(),
                pid: 0,
                job_id,
                command: command.clone(),
                cpu_pct,
                mem_gb,
                gpus,
                gpu_pct,
                gpumem_pct,
                gpumem_gb,
                cputime_sec,
                rolledup,
                cpu_util_pct
            }));
        }

        results.push((job_id, records));
    }

    results
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

            rmem_avg = (mem_avg * 100.0) / mem;
            rmem_peak = (mem_peak * 100.0) / mem;

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
