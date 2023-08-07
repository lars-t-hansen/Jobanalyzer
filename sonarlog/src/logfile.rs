/// Simple parser / preprocessor / input filterer for the Sonar log file format.
// TAGGED FORMAT
//
// These are the tagged fields with their gloss, contents, and defaults:
//
//  Name    Optional?  Gloss                                  Format, default
//  ------  ---------  -------------------------------------  --------------------------------------
//  v       No         Version number of program writing log  major.minor.bugfix
//  time    No         Timestamp                              ISO w/o subseconds, with UTC offset
//  host    No         Host name                              Alphanumeric FQDN
//  cores   Yes        Number of cores on the system          Positive integer, default 0
//  user    No         User name of user running job          Alphanumeric Unix user name
//  job     No         Job number                             Nonnegative integer
//  cmd     No         Command string                         Alphanumeric, maybe with spaces
//  cpu%    No         % of one core utilized at present      Nonnegative float
//  cpukib  No         KiB of node memory currently used      Nonnegative integer
//  gpus    Yes        Set of GPUs being used by job          "none", "unknown", list of positive
//                                                              integers, default "none"
//  gpu%    Yes        % of GPU cards utilized by job         Nonnegative float, default 0
//  gpumem% Yes        % of GPU cards utilized by job         Nonnegative float, default 0
//  gpukib  Yes        KiB of GPU memory currently used       Nonnegative float, default 0
//
// Note that these fields need not be in any particular order.  Per the definition in sonar, the
// `gpu%`, `gpumem%`, and `gpukib` fields are the shares of the job summed across / relative to all
// the cards in the `gpus` field.
//
// Percentages are represented in the input with 100% as "100" or "100.0".
//
//
// UNTAGGED FORMAT
//
// Prior to the introduction of tagged fields, these fields were present in the following order:
//
//  time, host, cores, user, job, cmd, cpu%, cpukib, gpus, gpu%, gpumem%, gpukib
//
// with gpus being a base-2 integer representing a bitmask of the cards being used, with "unknown"
// being represented as (usize)-1, usize being 64-bit.  In very old data there are no gpu fields.
//
//
// NOTE:
//
// - It's an important feature that a corrupted record is dropped silently.  (We can add a switch
//   to be noisy about it if that is useful for interactive log testing.)  The reason is that
//   appending-to-log is not atomic wrt reading-from-log and it is somewhat likely that there
//   will be situations where the analysis code runs into a partly-written (corrupted) record.
//
// - Tagged and untagged records can be mixed in a file in any order; this allows files to be
//   catenated and sonar to be updated at any time.
//
// - The format of `gpus` is under discussion as of 2023-07-31.
//
// - There's an assumption here that if the CSV decoder encounters illegal UTF8 - or for that matter
//   any other parse error, but bad UTF8 is a special case - it will make progress to the end of the
//   record anyway (as CSV is line-oriented).  This is a reasonable assumption but I've found no
//   documentation that guarantees it.
//
// TODO: parse_logfile should possibly take a Path, not a &str filename.  See comments in logtree.rs.
//
// TODO: Obscure test cases, notably I/O error and non-UTF8 input.
use crate::{parse_timestamp, LogEntry, Timestamp};

use anyhow::Result;
use serde::Deserialize;
use std::collections::HashSet;
use std::io::Write;
use std::str::FromStr;

/// Parse a log file into a set of LogEntry structures, applying an application-defined filter to
/// each record while reading.
///
/// This returns an error in the case of I/O errors, but silently drops records with parse errors.

pub fn parse_logfile<F>(file_name: &str, include_record: F) -> Result<Vec<LogEntry>>
where
    // (user, host, jobid, timestamp)
    F: Fn(&str, &str, u32, &Timestamp) -> bool,
{
    #[derive(Debug, Deserialize)]
    struct LogRecord {
        fields: Vec<String>,
    }

    let mut results = vec![];

    // An error here is going to be an I/O error so always propagate it.
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_path(file_name)?;

    'outer: for deserialized_record in reader.deserialize::<LogRecord>() {
        match deserialized_record {
            Err(e) => {
                if e.is_io_error() {
                    return Err(e.into());
                }
                // Otherwise drop the record
                continue 'outer;
            }
            Ok(record) => {
                // Find the fields and then convert them.  Duplicates are not allowed.  Mandatory
                // fields are really required.
                let mut version: Option<String> = None;
                let mut timestamp: Option<Timestamp> = None;
                let mut hostname: Option<String> = None;
                let mut num_cores: Option<u32> = None;
                let mut user: Option<String> = None;
                let mut job_id: Option<u32> = None;
                let mut command: Option<String> = None;
                let mut cpu_pct: Option<f64> = None;
                let mut mem_gb: Option<f64> = None;
                let mut gpus: Option<Option<HashSet<u32>>> = None;
                let mut gpu_pct: Option<f64> = None;
                let mut gpumem_pct: Option<f64> = None;
                let mut gpumem_gb: Option<f64> = None;

                if let Ok(t) = parse_timestamp(&record.fields[0]) {
                    // This is an untagged record
                    if record.fields.len() != 12 {
                        continue 'outer;
                    }
                    let mut failed;
                    version = Some("0.6.0".to_string());
                    timestamp = Some(t);
                    hostname = Some(record.fields[1].to_string());
                    (num_cores, failed) = get_u32(&record.fields[2]);
                    if failed {
                        continue 'outer;
                    }
                    user = Some(record.fields[3].to_string());
                    (job_id, failed) = get_u32(&record.fields[4]);
                    if failed {
                        continue 'outer;
                    }
                    command = Some(record.fields[5].to_string());
                    (cpu_pct, failed) = get_f64(&record.fields[6], 1.0);
                    if failed {
                        continue 'outer;
                    }
                    (mem_gb, failed) = get_f64(&record.fields[7], 1.0 / (1024.0 * 1024.0));
                    if failed {
                        continue 'outer;
                    }
                    (gpus, failed) = get_gpus_from_bitvector(&record.fields[8]);
                    if failed {
                        continue 'outer;
                    }
                    (gpu_pct, failed) = get_f64(&record.fields[9], 1.0);
                    if failed {
                        continue 'outer;
                    }
                    (gpumem_pct, failed) = get_f64(&record.fields[10], 1.0);
                    if failed {
                        continue 'outer;
                    }
                    (gpumem_gb, failed) = get_f64(&record.fields[11], 1.0 / (1024.0 * 1024.0));
                    if failed {
                        continue 'outer;
                    }
                } else {
                    // This must be a tagged record
                    for field in record.fields {
                        // TODO: Performance: Would it be better to extract the keyword, hash
                        // it, extract a code for it from a hash table, and then switch on that?
                        // It's bad either way.  Or we could run a state machine across the
                        // string here, that would likely be best.
                        let mut failed = false;
                        if field.starts_with("v=") {
                            if version.is_some() {
                                continue 'outer;
                            }
                            version = Some(field[2..].to_string())
                        } else if field.starts_with("time=") {
                            if timestamp.is_some() {
                                continue 'outer;
                            }
                            if let Ok(t) = parse_timestamp(&field[5..]) {
                                timestamp = Some(t.into());
                            } else {
                                continue 'outer;
                            }
                        } else if field.starts_with("host=") {
                            if hostname.is_some() {
                                continue 'outer;
                            }
                            hostname = Some(field[5..].to_string())
                        } else if field.starts_with("cores=") {
                            if num_cores.is_some() {
                                continue 'outer;
                            }
                            (num_cores, failed) = get_u32(&field[6..]);
                        } else if field.starts_with("user=") {
                            if user.is_some() {
                                continue 'outer;
                            }
                            user = Some(field[5..].to_string())
                        } else if field.starts_with("job=") {
                            if job_id.is_some() {
                                continue 'outer;
                            }
                            (job_id, failed) = get_u32(&field[4..]);
                        } else if field.starts_with("cmd=") {
                            if command.is_some() {
                                continue 'outer;
                            }
                            command = Some(field[4..].to_string())
                        } else if field.starts_with("cpu%=") {
                            if cpu_pct.is_some() {
                                continue 'outer;
                            }
                            (cpu_pct, failed) = get_f64(&field[5..], 1.0);
                        } else if field.starts_with("cpukib=") {
                            if mem_gb.is_some() {
                                continue 'outer;
                            }
                            (mem_gb, failed) = get_f64(&field[7..], 1.0 / (1024.0 * 1024.0));
                        } else if field.starts_with("gpus=") {
                            if gpus.is_some() {
                                continue 'outer;
                            }
                            (gpus, failed) = get_gpus_from_list(&field[5..]);
                        } else if field.starts_with("gpu%=") {
                            if gpu_pct.is_some() {
                                continue 'outer;
                            }
                            (gpu_pct, failed) = get_f64(&field[5..], 1.0);
                        } else if field.starts_with("gpumem%=") {
                            if gpumem_pct.is_some() {
                                continue 'outer;
                            }
                            (gpumem_pct, failed) = get_f64(&field[8..], 1.0);
                        } else if field.starts_with("gpukib=") {
                            if gpumem_gb.is_some() {
                                continue 'outer;
                            }
                            (gpumem_gb, failed) = get_f64(&field[7..], 1.0 / (1024.0 * 1024.0));
                        } else {
                            // Unknown field, ignore it silently, this is benign (mostly - it could
                            // be a field whose tag was chopped off, so maybe we should look for
                            // `=`).
                        }
                        if failed {
                            continue 'outer;
                        }
                    }
                }

                // Check that mandatory fields are present.

                if version.is_none()
                    || timestamp.is_none()
                    || hostname.is_none()
                    || user.is_none()
                    || job_id.is_none()
                    || command.is_none()
                    || cpu_pct.is_none()
                    || mem_gb.is_none()
                {
                    continue 'outer;
                }

                // Fill in default data for optional fields.

                if gpus.is_none() {
                    gpus = Some(Some(HashSet::new()))
                }
                if gpu_pct.is_none() {
                    gpu_pct = Some(0.0)
                }
                if gpumem_pct.is_none() {
                    gpumem_pct = Some(0.0)
                }
                if gpumem_gb.is_none() {
                    gpumem_gb = Some(0.0)
                }

                // Filter it

                if !include_record(
                    &user.as_ref().unwrap(),
                    &hostname.as_ref().unwrap(),
                    job_id.unwrap(),
                    &timestamp.unwrap(),
                ) {
                    continue 'outer;
                }

                // Ship it!

                results.push(LogEntry {
                    version: version.unwrap(),
                    timestamp: timestamp.unwrap(),
                    hostname: hostname.unwrap(),
                    num_cores: num_cores.unwrap(),
                    user: user.unwrap(),
                    job_id: job_id.unwrap(),
                    command: command.unwrap(),
                    cpu_pct: cpu_pct.unwrap(),
                    mem_gb: mem_gb.unwrap(),
                    gpus: gpus.unwrap(),
                    gpu_pct: gpu_pct.unwrap(),
                    gpumem_pct: gpumem_pct.unwrap(),
                    gpumem_gb: gpumem_gb.unwrap(),
                });
            }
        }
    }
    std::io::stdout().flush().unwrap();
    Ok(results)
}

fn get_u32(s: &str) -> (Option<u32>, bool) {
    if let Ok(n) = u32::from_str(s) {
        (Some(n), false)
    } else {
        (None, true)
    }
}

fn get_f64(s: &str, scale: f64) -> (Option<f64>, bool) {
    if let Ok(n) = f64::from_str(s) {
        (Some(n * scale), false)
    } else {
        (None, true)
    }
}

fn get_gpus_from_bitvector(s: &str) -> (Option<Option<HashSet<u32>>>, bool) {
    match usize::from_str_radix(s, 2) {
        Ok(mut bit_mask) => {
            let mut gpus = None;
            if bit_mask != 0 {
                let mut set = HashSet::new();
                if bit_mask != !0usize {
                    let mut shift = 0;
                    while bit_mask != 0 {
                        if (bit_mask & 1) != 0 {
                            set.insert(shift);
                        }
                        shift += 1;
                        bit_mask >>= 1;
                    }
                }
                gpus = Some(set);
            }
            (Some(gpus), false)
        }
        Err(_) => (None, true),
    }
}

fn get_gpus_from_list(s: &str) -> (Option<Option<HashSet<u32>>>, bool) {
    if s == "unknown" {
        (Some(Some(HashSet::new())), false)
    } else if s == "none" {
        (Some(None), false)
    } else {
        let mut set = HashSet::new();
        let vs: std::result::Result<Vec<_>, _> = s.split(',').map(u32::from_str).collect();
        match vs {
            Err(_) => (None, true),
            Ok(vs) => {
                for v in vs {
                    set.insert(v);
                }
                (Some(Some(set)), false)
            }
        }
    }
}

#[cfg(test)]
fn filter(_user: &str, _host: &str, _job: u32, _t: &Timestamp) -> bool {
    true
}

#[test]
fn test_parse_logfile1() {
    // No such directory
    assert!(parse_logfile("../sonar_test_data77/2023/05/31/xyz.csv", &filter).is_err());

    // No such file
    assert!(parse_logfile("../sonar_test_data0/2023/05/31/ml2.hpc.uio.no.csv", &filter).is_err());
}

#[test]
fn test_parse_logfile2() {
    // This file has four records, the second has a timestamp that is out of range and the fourth
    // has a timestamp that is malformed.
    let x = parse_logfile("../sonar_test_data0/other/bad_timestamp.csv", &filter).unwrap();
    assert!(x.len() == 2);
    assert!(x[0].user == "root");
    assert!(x[1].user == "riccarsi");
}

#[test]
fn test_parse_logfile3() {
    // This file has three records, the third has a GPU mask that is malformed.
    let x = parse_logfile("../sonar_test_data0/other/bad_gpu_mask.csv", &filter).unwrap();
    assert!(x.len() == 2);
    assert!(x[0].user == "root");
    assert!(x[1].user == "riccarsi");
}

#[test]
fn test_parse_logfile4() {
    let filter = |user: &str, _host: &str, _job: u32, _t: &Timestamp| user == "riccarsi";
    let x = parse_logfile("../sonar_test_data0/2023/05/30/ml8.hpc.uio.no.csv", &filter).unwrap();
    assert!(x.len() == 463);
}

#[test]
fn test_parse_logfile5() {
    // Tagged data
    let x = parse_logfile("../sonar_test_data0/2023/06/05/ml4.hpc.uio.no.csv", &filter).unwrap();
    assert!(x.len() == 3);
    assert!(x[0].user == "zabbix");
    assert!(x[1].user == "root");
    assert!(x[2].user == "larsbent");
    assert!(x[0].timestamp < x[1].timestamp);
    assert!(x[1].timestamp == x[2].timestamp);
    assert!(x[2].gpus == Some(HashSet::from([4, 5, 6])));
}
