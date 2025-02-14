/// Helpers for merging sample streams.

use crate::{hosts, empty_gpuset, union_gpuset, LogEntry, InputStreamSet, Timestamp};

use std::boxed::Box;
use std::collections::{HashMap, HashSet};
use std::iter::Iterator;

/// A bag of merged streams.  The constraints on the individual streams in terms of uniqueness and
/// so on depends on how they were merged and are not implied by the type.

pub type MergedSampleStreams = Vec<Vec<Box<LogEntry>>>;

/// Merge streams that have the same host and job ID into synthesized data.
///
/// Each output stream is sorted ascending by timestamp.  No two records have exactly the same time.
/// All records within a stream have the same host, command, user, and job ID.
///
/// The command name for synthesized data collects all the commands that went into the synthesized stream.

pub fn merge_by_host_and_job(mut streams: InputStreamSet) -> MergedSampleStreams {
    // The value is a set of command names and a vector of the individual streams.
    let mut collections: HashMap<(String, u32), (HashSet<String>, Vec<Vec<Box<LogEntry>>>)> =
        HashMap::new();

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

    let mut vs : MergedSampleStreams = vec![];
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

/// Merge streams that have the same job ID (across hosts) into synthesized data.
///
/// Each output stream is sorted ascending by timestamp.  No two records have exactly the same time.
/// All records within an output stream have the same host name, job ID, command name, and user.
///
/// The command name for synthesized data collects all the commands that went into the synthesized stream.
/// The host name for synthesized data collects all the hosts that went into the synthesized stream.

pub fn merge_by_job(mut streams: InputStreamSet) -> MergedSampleStreams {
    // The value is a set of command names, a set of host names, and a vector of the individual streams.
    let mut collections: HashMap<u32, (HashSet<String>, HashSet<String>, Vec<Vec<Box<LogEntry>>>)> =
        HashMap::new();

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

    let mut vs : MergedSampleStreams = zero;
    for (job_id, (mut cmds, mut hosts, streams)) in collections.drain() {
        let hostname = hosts::combine_hosts(hosts.drain().collect::<Vec<String>>());
        let cmdname = cmds.drain().collect::<Vec<String>>().join(",");
        // Any user from any record is fine.  There should be an invariant that no stream is empty,
        // so this should always be safe.
        let user = streams[0][0].user.clone();
        vs.push(merge_streams(hostname, cmdname, user, job_id, streams));
    }

    vs
}

/// Merge streams that have the same host (across jobs) into synthesized data.
///
/// Each output stream is sorted ascending by timestamp.  No two records have exactly the same time.
/// All records within an output stream have the same host name, job ID, command name, and user.
///
/// The command name and user name for synthesized data are "_merged_".  It would be possible to do
/// something more interesting, such as aggregating them.
///
/// The job ID for synthesized data is 0, which is not ideal but probably OK so long as the consumer
/// knows it.

pub fn merge_by_host(mut streams: InputStreamSet) -> MergedSampleStreams {
    // The key is the host name.
    let mut collections: HashMap<String, Vec<Vec<Box<LogEntry>>>> = HashMap::new();

    streams
        .drain()
        .for_each(|((host, _, _), v)| {
            // This lumps jobs with job ID 0 in with the others.
            if let Some(vs) = collections.get_mut(&host) {
                vs.push(v);
            } else {
                collections.insert(host, vec![v]);
            }
        });

    let mut vs : MergedSampleStreams = vec![];
    for (hostname, streams) in collections.drain() {
        let cmdname = "_merged_".to_string();
        let username = "_merged_".to_string();
        let job_id = 0;
        vs.push(merge_streams(hostname, cmdname, username, job_id, streams));
    }

    vs
}

// What does it mean to sample a job that runs on multiple hosts, or to sample a host that runs
// multiple jobs concurrently?
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

fn merge_streams(
    hostname: String,
    command: String,
    username: String,
    job_id: u32,
    streams: Vec<Vec<Box<LogEntry>>>,
) -> Vec<Box<LogEntry>> {
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
            if !have_smallest ||
                streams[smallest_stream][indices[smallest_stream]].timestamp > streams[i][indices[i]].timestamp {
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
        let near_past = min_time - chrono::Duration::seconds(30);
        let deep_past = min_time - chrono::Duration::seconds(60);

        // Now select values from all streams (either a value in the time window or the most
        // recent value before the time window) and advance the stream pointers for the ones in
        // the window.
        let mut selected : Vec<&Box<LogEntry>> = vec![];
        for i in 0..streams.len() {
            let s = &streams[i];
            let ix = indices[i];
            let lim = s.len();
            if ix < lim && s[ix].timestamp >= min_time && s[ix].timestamp < lim_time {
                // Current exists and is in in the time window, pick it up and advance index
                selected.push(&s[ix]);
                indices[i] += 1;
            } else if ix > 0 && ix < lim && s[ix-1].timestamp >= near_past {
                // Previous exists and is not last and is in the near past, pick it up.  The
                // condition is tricky.  ix > 0 guarantees that there is a past record at ix - 1,
                // while ix < lim says that there is also a future record at ix.
                //
                // This is hard to make reliable.  The guard on the time is necessary to avoid
                // picking up records from a lot of dead processes.  Intra-host it is OK.
                // Cross-host it depends on sonar runs being more or less synchronized.
                selected.push(&s[ix-1]);
            } else if ix > 0 && s[ix-1].timestamp < min_time && s[ix-1].timestamp >= deep_past {
                // Previous exists (and is last) and is not in the deep past, pick it up
                selected.push(&s[ix-1]);
            } else {
                // Various cases where we don't pick up any data:
                // - we're at the first position and the record is in the future
                // - we're at the last position and the record is in the deep past
            }
        }

        records.push(sum_records("0.0.0".to_string(), min_time, hostname.clone(), username.clone(), job_id, command.clone(), &selected));
    }

    records
}

fn sum_records(
    version: String,
    timestamp: Timestamp,
    hostname: String,
    user: String,
    job_id: u32,
    command: String,
    selected: &[&Box<LogEntry>],
) -> Box<LogEntry> {
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
    Box::new(LogEntry {
        version,
        timestamp,
        hostname,
        num_cores: 0,
        user,
        pid: 0,
        job_id,
        command,
        cpu_pct,
        mem_gb,
        gpus,
        gpu_pct,
        gpumem_pct,
        gpumem_gb,
        cputime_sec,
        rolledup,
        cpu_util_pct
    })
}

pub fn fold_samples_hourly(samples: Vec<Box<LogEntry>>) -> Vec<Box<LogEntry>> {
    fold_samples(samples, crate::truncate_to_hour)
}

pub fn fold_samples_daily(samples: Vec<Box<LogEntry>>) -> Vec<Box<LogEntry>> {
    fold_samples(samples, crate::truncate_to_day)
}

fn fold_samples<'a>(samples: Vec<Box<LogEntry>>, get_time: fn(Timestamp) -> Timestamp) -> Vec<Box<LogEntry>> {
    let mut result = vec![];
    let mut i = 0;
    while i < samples.len() {
        let s0 = &samples[i];
        let t0 = get_time(s0.timestamp);
        i += 1;
        let mut bucket = vec![s0];
        while i < samples.len() && get_time(samples[i].timestamp) == t0 {
            bucket.push(&samples[i]);
            i += 1;
        }
        let mut r = sum_records(
            "0.0.0".to_string(),
            t0,
            s0.hostname.clone(),
            "_merged_".to_string(),
            0,
            "_merged_".to_string(),
            &bucket);
        let n = bucket.len() as f64;
        r.cpu_pct /= n;
        r.mem_gb /= n;
        r.gpu_pct /= n;
        r.gpumem_pct /= n;
        r.gpumem_gb /= n;
        r.cputime_sec /= n;
        r.cpu_util_pct /= n;
        result.push(r);
    }

    result
}
