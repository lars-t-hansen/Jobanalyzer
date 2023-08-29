/// Compute jobs aggregates from a set of log entries.

use crate::format;
use crate::prjobs;
use crate::{JobFilterAndAggregationArgs, JobPrintArgs, MetaArgs};

use anyhow::Result;
use sonarlog::{self, empty_gpuset, union_gpuset, LogEntry, StreamKey, Timestamp};
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

pub fn aggregate_and_print_jobs(
    output: &mut dyn io::Write,
    system_config: &Option<HashMap<String, sonarlog::System>>,
    filter_args: &JobFilterAndAggregationArgs,
    print_args: &JobPrintArgs,
    meta_args: &MetaArgs,
    streams: HashMap<StreamKey, Vec<Box<LogEntry>>>,
    earliest: Timestamp,
    latest: Timestamp,
) -> Result<()> {
    let jobvec =
        aggregate_and_filter_jobs(system_config, filter_args, streams, earliest, latest);

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
    system_config: &Option<HashMap<String, sonarlog::System>>,
    filter_args: &JobFilterAndAggregationArgs,
    streams: HashMap<StreamKey, Vec<Box<LogEntry>>>,
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

    // Select streams and synthesize a merged stream, and then aggregate and print it.

    let mut jobs = 
        if filter_args.batch {
            merge_by_job(streams)
        } else {
            merge_by_host_and_job(streams)
        };
    jobs
        .drain(0..)
        .filter(|job| job.len() >= min_samples)
        .map(|job| (aggregate_job(system_config, &job, earliest, latest), job))
        .filter(&aggregate_filter)
        .collect::<Vec<(JobAggregate, Vec<Box<LogEntry>>)>>()
}

// Merge streams that have the same host and job ID into synthesized data.
// The command name for synthesized data collects all the commands that went into the synthesized stream.

fn merge_by_host_and_job(mut streams: HashMap<StreamKey, Vec<Box<LogEntry>>>) -> Vec<Vec<Box<LogEntry>>> {
    // The value is a set of command names and a vector of the individual streams.
    let mut collections: HashMap<(String, u32), (HashSet<String>, Vec<Vec<Box<LogEntry>>>)> = HashMap::new();

    // The value is a map (by host) of the individual streams with job ID zero, these can't be
    // merged and must just be passed on.
    let mut zero: HashMap<String, Vec<Vec<Box<LogEntry>>>> = HashMap::new();
    
    streams
        .drain()
        .for_each(|((host, _, cmd), v)| {
            let id = v[0].job_id;
            if id == 0 {
                if let Some(vs) = zero.get_mut(&host) {
                    vs.push(v);
                } else {
                    zero.insert(host.clone(), vec![v]);
                }
            } else {
                let key = (host, id);
                if let Some((cmds, vs)) = collections.get_mut(&key) {
                    cmds.insert(cmd);
                    vs.push(v);
                } else {
                    let mut cmds = HashSet::new();
                    cmds.insert(cmd);
                    collections.insert(key, (cmds, vec![v]));
                }
            }
        });

    let mut vs : Vec<Vec<Box<LogEntry>>> = vec![]; 
    for ((hostname, job_id), (mut cmds, streams)) in collections.drain() {
        if let Some(zeroes) = zero.remove(&hostname) {
            vs.extend(zeroes);
        }
        let cmdname = cmds.drain().collect::<Vec<String>>().join(",");
        // Any user from any record is fine.  There should be an invariant that no stream is empty,
        // so this should always be safe.
        let user = streams[0][0].user.clone();
        vs.push(merge_streams(hostname, cmdname, user, job_id, streams));
    }

    vs
}

// Merge streams that have the same job ID (across hosts) into synthesized data.
// The command name for synthesized data collects all the commands that went into the synthesized stream.
// The host name for synthesized data collects all the hosts that went into the synthesized stream.

fn merge_by_job(mut streams: HashMap<StreamKey, Vec<Box<LogEntry>>>) -> Vec<Vec<Box<LogEntry>>> {
    // The value is a set of command names, a set of host names, and a vector of the individual streams.
    let mut collections: HashMap<u32, (HashSet<String>, HashSet<String>, Vec<Vec<Box<LogEntry>>>)> = HashMap::new();

    // The value is a vector of the individual streams with job ID zero, these can't be merged and
    // must just be passed on.
    let mut zero: Vec<Vec<Box<LogEntry>>> = vec![];
    
    streams
        .drain()
        .for_each(|((host, _, cmd), v)| {
            let id = v[0].job_id;
            if id == 0 {
                zero.push(v);
            } else {
                let key = id;
                if let Some((cmds, hosts, vs)) = collections.get_mut(&key) {
                    cmds.insert(cmd);
                    hosts.insert(host);
                    vs.push(v);
                } else {
                    let mut cmds = HashSet::new();
                    cmds.insert(cmd);
                    let mut hosts = HashSet::new();
                    hosts.insert(host);
                    collections.insert(key, (cmds, hosts, vec![v]));
                }
            }
        });

    let mut vs : Vec<Vec<Box<LogEntry>>> = zero;
    for (job_id, (mut cmds, mut hosts, streams)) in collections.drain() {
        let hostname = format::combine_hosts(hosts.drain().collect::<Vec<String>>());
        let cmdname = cmds.drain().collect::<Vec<String>>().join(",");
        // Any user from any record is fine.  There should be an invariant that no stream is empty,
        // so this should always be safe.
        let user = streams[0][0].user.clone();
        vs.push(merge_streams(hostname, cmdname, user, job_id, streams));
    }

    vs
}

// What does it mean to sample a job that runs on multiple hosts?
//
// Consider peak CPU utilization.  The single-host interpretation of this is the highest valued
// sample for CPU utilization across the run (sample stream).  For cross-host jobs we want the
// highest valued sum-of-samples (for samples taken at the same time) for CPU utilization across the
// run.  However, in general samples will not have been taken on different hosts at the same time so
// this is not completely trivial.
//
// Consider all sample streams from all hosts in the job in parallel, here "+" denotes a sample and
// "-" denotes time just passing, we have three cores C1 C2 C3, and each character is one time tick:
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
// Thus a cross-host sample stream is a vector of these synthesized samples. The synthesized
// LogEntries that we create will have aggregate host sets (effectively just an aggregate host name
// that is the same value in every record) and gpu sets (just a union).
//
// Algorithm:
//
//  given vector V of sample streams for a set of hosts and a common job ID:
//  given vector A of "current observed values for all streams", initially "0"
//  while some streams in V are not empty
//     get lowest time  (*) (**) across nonempty streams of V
//     update A with values from the those streams
//     advance those streams
//     push out a new sample record with current values
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
// Our normal aggregation logic can be run on the synthesized sample stream.
//
// merge_streams() takes a set of streams for an individual job (along with names for the host, the
// command, the user, and the job) and returns a single, merged stream for the job, where the
// synthesized records for a single job all have the following artifacts.  Let R be the records that
// went into synthesizing a single record according to the algorithm above and S be all the input
// records for the job.  Then:
//
//   - version is "0.0.0".
//   - hostname, command,  user, and job_id are as given to the function
//   - timestamp is synthesized from the timestamps of R
//   - num_cores is 0
//   - pid is 0
//   - cpu_pct is the sum across the cpu_pct of R
//   - mem_gb is the sum across the mem_gb of R
//   - gpus is the union of the gpus across R
//   - gpu_pct is the sum across the gpu_pct of R
//   - gpumem_pct is the sum across the gpumem_pct of R
//   - gpumem_gb is the sum across the gpumem_gb of R
//   - cputime_sec is the sum across the cputime_sec of R
//   - rolledup is the number of records in the list
//   - cpu_util_pct is the sum across the cpu_util_pct of R (roughly the best we can do)
//
// Invariants of the input that are used:
//
// - streams are never empty
// - streams are sorted by ascending timestamp
// - in no stream are there two adjacent records with the same timestamp
//
// Invariants not used:
//
// - records may be obtained from the same host and the streams may therefore be synchronized

fn merge_streams(hostname: String, command: String, username: String, job_id: u32, streams: Vec<Vec<Box<LogEntry>>>) -> Vec<Box<LogEntry>> {
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

        // Now select values from all streams (either a value in the time window or the most
        // recent value before the time window) and advance the stream pointers for the ones in
        // the window.
        let mut selected : Vec<&Box<LogEntry>> = vec![];
        for i in 0..streams.len() {
            if indices[i] < streams[i].len() && streams[i][indices[i]].timestamp >= min_time && streams[i][indices[i]].timestamp < lim_time {
                // Advance those that are in the time window
                selected.push(&streams[i][indices[i]]);
                indices[i] += 1;
            } else if indices[i] > 0 && streams[i][indices[i]-1].timestamp < min_time {
                // Pick up the ones that are in the most recent past
                selected.push(&streams[i][indices[i]-1]);
            } else {
                // Nothing: in this case, there may be a record but it is in the future
            }
        }

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

        // Synthesize the record.
        records.push(Box::new(LogEntry {
            version: "0.0.0".to_string(),
            timestamp: min_time,
            hostname: hostname.clone(),
            num_cores: 0,
            user: username.clone(),
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

    records
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
