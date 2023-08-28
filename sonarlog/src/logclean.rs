/// Postprocess and clean up log data after ingestion.

use crate::{LogEntry, System, Timestamp};

use chrono::Duration;
use std::boxed::Box;
use std::collections::HashMap;

#[cfg(test)]
use crate::read_logfiles;

/// Apply postprocessing to the records in the array:
///
/// - compute the cpu_util_pct field from cputime_sec and timestamp for consecutive records
/// - remove records for which the filter function returns false
/// - if `configs` is not None and there is the necessary information for a given host, clean up the
///   gpumem_pct and gpumem_gb fields so that they are internally consistent
///
/// Returns the truncated array.  The returned records are sorted by hostname (major key) and
/// timestamp (minor key).

pub fn postprocess_log<F>(
    mut entries: Vec<Box<LogEntry>>,
    filter: F,
    configs: Option<&HashMap<String, System>>,
) -> Vec<Box<LogEntry>>
where
    F: Fn(&LogEntry) -> bool
{
    // Sort by hostname first and timestamp within the host.  There will normally be runs of records
    // with the same timestamp but the order within each run is not important.
    entries.sort_by(|a, b| {
        if a.hostname == b.hostname {
            a.timestamp.cmp(&b.timestamp)
        } else {
            a.hostname.cmp(&b.hostname)
        }
    });

    // For each record, compute the cpu_util_pct field.  We look for runs of records from the same
    // host.  Then in that run, we look for runs of records with the same timestamp.  There may be
    // multiple records per job in that run, and they may or may not also have the same cmd, and
    // they may or may not have been rolled up.  We need to construct a history s.t. we can compute
    // the difference between the "new" reading for a record and the "previous" reading.  There are
    // two cases:
    //
    // - If the job is not rolled-up then we know that for a given pid there is only ever one
    //   record at a given time.
    //
    // - If the job is rolled-up then we know that for a given (job_id, cmd) pair there is only one
    //   record, but job_id by itself is not enough to distinguish records, and there is no obvious
    //   distinguishing pid value, as the set of rolled-up processes may change from invocation to
    //   invocation of sonar.  We also know a rolled-up record has rolledup > 0.
    //
    // Therefore, let the pid for a rolled-up record r be 10e7 + r.job_id (Linux pids are smaller
    // than 10e7).  Then (pid, cmd) is enough to distinguish a record always, though it is more
    // complicated than necessary for non-rolled-up jobs.
    //
    // A hashtable maintains information about the previously highest seen timestamp for a (pid,cmd)
    // on a host.  When we process a job record, we look to this table to get the time difference
    // and the cputime difference, and can compute the cputime sample for the record.

    // Process runs of host names
    let mut h_start = 0;
    let mut h_end = h_start;
    while h_start < entries.len() {

        // Find the end of the run of hostnames
        while h_end < entries.len() && entries[h_start].hostname == entries[h_end].hostname {
            h_end += 1;
        }

        // Records for the host are now in [h_start,h_end).

        // The hasmap will map (pid, cmd) -> (t, c) where t is the previous timestamp and c is the
        // cputime_sec field for the pid at that time.
        //
        // TODO: Unfortunately (due to borrowing rules) we need to use a String for a key, which
        // will lead to a lot of string allocation in this loop.  To do better I think we need to
        // keep our strings out of the main data structures and in a separate string table, see eg
        // https://docs.rs/ustr/latest/ustr/index.html.
        let mut last_seen : HashMap<(u32, String), (Timestamp, f64)> = HashMap::new();

        // Process runs of time stamps
        let mut t_start = h_start;
        let mut t_end = t_start;
        while t_start < h_end {
            // Find the end of the run of time stamps, and process records while we go.
            while t_end < h_end && entries[t_start].timestamp == entries[t_end].timestamp {
                // If we know a previous time for the record, then we can compute the cpu usage
                // since then as the difference in cpu usage divided by the difference in time.
                // Otherwise we start out by setting the cpu usage to cpu_pct, which is as good an
                // approximation as we'll get.
                //
                // There is an invariant here that within each run of records with the same
                // timestamp and host, there is at most one record for each (synthetic_pid, cmd)
                // pair.  This invariant allows us to update the hash table within the loop rather
                // than having a second loop directly after.  The invariant is made possible by
                // Sonar and by the computation of synthetic_pid.
                let synthetic_pid = if entries[t_end].rolledup > 0 {
                    1000000 + entries[t_end].job_id
                } else {
                    entries[t_end].pid
                };
                let key = (synthetic_pid, entries[t_end].command.clone());
                entries[t_end].cpu_util_pct = 
                    if let Some((ref mut t, ref mut c)) = last_seen.get_mut(&key) {
                        let dt = ((entries[t_end].timestamp - *t) as Duration).num_seconds() as f64;
                        let dc = entries[t_end].cputime_sec - *c;
                        *t = entries[t_end].timestamp;
                        *c = entries[t_end].cputime_sec;
                        (dc / dt) * 100.0
                    } else {
                        last_seen.insert(key, (entries[t_end].timestamp, entries[t_end].cputime_sec));
                        entries[t_end].cpu_pct
                    };
                t_end += 1;
            }

            // Next run of timestamps
            t_start = t_end;
        }

        // Next run of hosts
        h_start = h_end;
    }

    // Remove elements that don't pass the filter and pack the array.
    let mut dst = 0;
    for src in 0..entries.len() {
        if filter(&entries[src]) {
            entries.swap(dst, src);
            dst += 1;
        }
    }
    entries.truncate(dst);

    // Clean up the gpumem_pct and gpumem_gb fields based on system information, if available.
    if let Some(confs) = configs {
        for entry in &mut entries {
            if let Some(conf) = confs.get(&entry.hostname) {
                let cardsize = (conf.gpumem_gb as f64) / (conf.gpu_cards as f64);
                if conf.gpumem_pct {
                    entry.gpumem_gb = entry.gpumem_pct * cardsize;
                } else {
                    entry.gpumem_pct = entry.gpumem_gb / cardsize;
                }
            }
        }
    }

    entries
}

#[test]
fn test_postprocess_log_cpu_util_pct() {
    // This file has field names, cputime_sec, pid, and rolledup
    let (entries, _, _, _) = read_logfiles(&vec!["../sonar_test_data0/2023/06/05/ml4.hpc.uio.no.csv".to_string()]).unwrap();
    let len = entries.len();
    let any = |e:&LogEntry| e.user != "root";
    let new_entries = postprocess_log(entries, any, None);

    // Filtering removed one entry
    assert!(new_entries.len() == len-1);

    // Check sorted
    for i in 1..new_entries.len() {
        assert!(new_entries[i-1].hostname <= new_entries[i].hostname);
        assert!(new_entries[i-1].timestamp <= new_entries[i].timestamp || new_entries[i-1].hostname < new_entries[i].hostname);
    }

    // This test is brittle!
    //
    // For this pid (1090) there are three records for ml4, pairwise 300 seconds apart (and
    // disordered in the input), and the cputime_sec field for the second record is 300 seconds
    // higher, giving us 100% utilization for that time window, and for the third record 150 seconds
    // higher, giving us 50% utilization for that window.
    assert!(new_entries[2].cpu_util_pct == 1473.7); // The cpu_pct value
    assert!(new_entries[3].cpu_util_pct == 100.0);
    assert!(new_entries[4].cpu_util_pct == 50.0);
    // This has the same pid *but* a different host, so the utilization for the first record should
    // once again be set to the cpu_pct value.
    assert!(new_entries[5].cpu_util_pct == 128.0);
}
