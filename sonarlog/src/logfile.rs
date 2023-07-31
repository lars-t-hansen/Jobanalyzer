// Simple parser / preprocessor / input filterer for the Sonar log file format.
//
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
//  gpu%    Yes        % of GPU cards utilized by job         Nonnegative float, default 0.0
//  gpumem% Yes        % of GPU cards utilized by job         Nonnegative float, default 0.0
//  gpukib  Yes        KiB of GPU memory currently used       Nonnegative integer, default 0
//
// Note that these fields need not be in any particular order.  Per sonar, the `gpu%`, `gpumem%`,
// and `gpukib` fields are summed across / relative to the cards in the `gpus` field.
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
// - We assume that when a record has a tagged field then all the fields in the record are tagged,
//   ergo, the first field will be tagged if any field is tagged.
//
// - We further assume that the first record in a file determines the format of the remaining
//   records in the file.
//
// - The format of `gpus` is under discussion as of 2023-07-31: both the overall format, and whether
//   the card numbers start at 0 or 1.
//
// - There's an assumption here that if the CSV decoder encounters illegal UTF8 - or for that matter
//   any other parse error, but bad UTF8 is a special case - it will make progress to the end of the
//   record anyway (as CSV is line-oriented).  This is a reasonable assumption but I've found no
//   documentation that guarantees it.
//
// TODO: parse_logfile should possibly take a Path, not a &str filename.  See comments in logtree.rs.
//
// TODO: Obscure test cases, notably I/O error and non-UTF8 input.

use crate::LogEntry;
use anyhow::Result;
use chrono::prelude::DateTime;
use chrono::Utc;
use serde::Deserialize;
use std::collections::HashSet;
use std::str::FromStr;
use std::io::Write;

/// Parse a log file into a set of LogEntry structures, applying an application-defined filter to
/// each record while reading.
///
/// This returns an error in the case of I/O errors, but silently drops records with parse errors.

pub fn parse_logfile<F>(file_name: &str, include_record: F) -> Result<Vec<LogEntry>>
where
    // (user, host, jobid, timestamp)
    F: Fn(&str, &str, u32, &DateTime<Utc>) -> bool,
{
    #[derive(Debug, Deserialize)]
    struct LogRecord {
        fields: Vec<String>
    }

    let is_tagged = {
        // An error here is going to be an I/O error so always propagate it.
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_path(file_name)?;

        if let Some(first) = reader.deserialize::<LogRecord>().next() {
            // An error here is *super* annoying because it means there's some kind of decoding error.
            // In this case the record is illegal.  We really need to be probing the next one.
            //
            // TODO: Is this correct?  Could "v=" be the start of a user name or a host name or a
            // command?  Probably...  In fact, a command name could fake any one field.  So likely
            // we need to probe a little deeper here.
            first?.fields.iter().any(|x| x.starts_with("v="))
        } else {
            false
        }
    };
    if is_tagged {
        parse_tagged_logfile(file_name, include_record)
    } else {
        parse_untagged_logfile(file_name, include_record)
    }
}

pub fn parse_untagged_logfile<F>(file_name: &str, include_record: F) -> Result<Vec<LogEntry>>
where
    // (user, host, jobid, timestamp)
    F: Fn(&str, &str, u32, &DateTime<Utc>) -> bool,
{
    #[derive(Debug, Deserialize)]
    struct LogRecord {
        timestamp: String,
        hostname: String,
        num_cores: u32,
        user: String,
        job_id: u32,
        command: String,
        cpu_percentage: f64,
        mem_kb: u64,
        gpu_mask: String,
        gpu_percentage: f64,
        gpu_mem_percentage: f64,
        gpu_mem_kb: u64,
    }

    let mut results = vec![];

    // An error here is going to be an I/O error so always propagate it.
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(file_name)?;

    for deserialized_record in reader.deserialize::<LogRecord>() {
        match deserialized_record {
            Err(e) => {
                if e.is_io_error() {
                    return Err(e.into());
                }
                // Otherwise drop the record
            }
            Ok(record) => {
                match DateTime::parse_from_rfc3339(&record.timestamp) {
                    Err(_) => {
                        // Drop the record
                    }
                    Ok(t) => {
                        let timestamp: DateTime<Utc> = t.into();
                        if include_record(&record.user, &record.hostname, record.job_id, &timestamp)
                        {
                            match usize::from_str_radix(&record.gpu_mask, 2) {
                                Err(_) => {
                                    // Drop the record
                                }
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
                                    results.push(LogEntry {
                                        version: "0.6.0".to_string(),
                                        timestamp,
                                        hostname: record.hostname,
                                        num_cores: record.num_cores,
                                        user: record.user,
                                        job_id: record.job_id,
                                        command: record.command,
                                        cpu_pct: record.cpu_percentage / 100.0,
                                        mem_gb: (record.mem_kb as f64) / (1024.0 * 1024.0),
                                        gpus,
                                        gpu_pct: record.gpu_percentage / 100.0,
                                        gpu_mem_pct: record.gpu_mem_percentage / 100.0,
                                        gpu_mem_gb: (record.gpu_mem_kb as f64) / (1024.0 * 1024.0),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(results)
}

pub fn parse_tagged_logfile<F>(file_name: &str, include_record: F) -> Result<Vec<LogEntry>>
where
    // (user, host, jobid, timestamp)
    F: Fn(&str, &str, u32, &DateTime<Utc>) -> bool,
{
    #[derive(Debug, Deserialize)]
    struct LogRecord {
        fields: Vec<String>
    }

    let mut results = vec![];

    // An error here is going to be an I/O error so always propagate it.
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_path(file_name)?;

    'outer:
    for deserialized_record in reader.deserialize::<LogRecord>() {
        match deserialized_record {
            Err(e) => {
                if e.is_io_error() {
                    println!("I/O error");
                    return Err(e.into());
                }
                // Otherwise drop the record
                println!("Deserialize failed");
                continue 'outer;
            }
            Ok(record) => {
                // Find the fields and then convert them.  Duplicates are not allowed.  Mandatory
                // fields are really required.
                let mut version : Option<String> = None;
                let mut timestamp : Option<DateTime<Utc>> = None;
                let mut hostname : Option<String> = None;
                let mut num_cores : Option<u32> = None;
                let mut user : Option<String> = None;
                let mut job_id : Option<u32> = None;
                let mut command : Option<String> = None;
                let mut cpu_pct : Option<f64> = None;
                let mut mem_gb : Option<f64> = None;
                let mut gpus : Option<Option<HashSet<u32>>> = None;
                let mut gpu_pct : Option<f64> = None;
                let mut gpu_mem_pct : Option<f64> = None;
                let mut gpu_mem_gb : Option<f64> = None;

                for field in record.fields {
                    println!("{}", field);
                    // TODO: Performance: Would it be better to extract the keyword, hash
                    // it, extract a code for it from a hash table, and then switch on that?
                    // It's bad either way.  Or we could run a state machine across the
                    // string here, that would likely be best.
                    if field.starts_with("v=") {
                        if version.is_some() {
                            continue 'outer;
                        }
                        version = Some(field[2..].to_string())
                    } else if field.starts_with("time=") {
                        if timestamp.is_some() {
                            continue 'outer;
                        }
                        match DateTime::parse_from_rfc3339(&field[5..]) {
                            Err(_) => {
                                println!("Failed timestamp");
                                continue 'outer;
                            }
                            Ok(t) => {
                                timestamp = Some(t.into());
                            }
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
                        match u32::from_str(&field[6..]) {
                            Err(_) => {
                                println!("Failed cores");
                                continue 'outer;
                            }
                            Ok(v) => {
                                num_cores = Some(v)
                            }
                        }
                    } else if field.starts_with("user=") {
                        if user.is_some() {
                            continue 'outer;
                        }
                        user = Some(field[5..].to_string())
                    } else if field.starts_with("job=") {
                        if job_id.is_some() {
                            continue 'outer;
                        }
                        match u32::from_str(&field[4..]) {
                            Err(_) => {
                                println!("Failed job");
                                continue 'outer;
                            }
                            Ok(v) => {
                                job_id = Some(v)
                            }
                        }
                    } else if field.starts_with("cmd=") {
                        if command.is_some() {
                            continue 'outer;
                        }
                        command = Some(field[4..].to_string())
                    } else if field.starts_with("cpu%=") {
                        if cpu_pct.is_some() {
                            continue 'outer;
                        }
                        match f64::from_str(&field[5..]) {
                            Err(_) => {
                                println!("Failed cpu%");
                                continue 'outer;
                            }
                            Ok(v) => {
                                cpu_pct = Some(v / 100.0)
                            }
                        }
                    } else if field.starts_with("cpukib=") {
                        if mem_gb.is_some() {
                            continue 'outer;
                        }
                        match f64::from_str(&field[7..]) {
                            Err(_) => {
                                println!("Failed cpukib");
                                continue 'outer;
                            }
                            Ok(v) => {
                                mem_gb = Some(v / (1024.0 * 1024.0))
                            }
                        }
                    } else if field.starts_with("gpus=") {
                        if gpus.is_some() {
                            continue 'outer;
                        }
                        if &field[5..] == "unknown" {
                            gpus = Some(Some(HashSet::new()))
                        } else if &field[5..] == "none" {
                            gpus = Some(None);
                        } else {
                            let mut set = HashSet::new();
                            let vs : std::result::Result<Vec<_>,_> = field[5..].split(',').map(u32::from_str).collect();
                            match vs {
                                Err(_) => {
                                    println!("Failed gpus");
                                    continue 'outer
                                }
                                Ok(vs) => {
                                    for v in vs {
                                        set.insert(v);
                                    }
                                    gpus = Some(Some(set))
                                }
                            }
                        }
                    } else if field.starts_with("gpu%=") {
                        if gpu_pct.is_some() {
                            continue 'outer;
                        }
                        match f64::from_str(&field[5..]) {
                            Err(_) => {
                                println!("Failed gpu%");
                                continue 'outer;
                            }
                            Ok(v) => {
                                gpu_pct = Some(v / 100.0)
                            }
                        }
                    } else if field.starts_with("gpumem%=") {
                        if gpu_mem_pct.is_some() {
                            continue 'outer;
                        }
                        match f64::from_str(&field[8..]) {
                            Err(_) => {
                                println!("Failed gpumem%");
                                continue 'outer;
                            }
                            Ok(v) => {
                                gpu_mem_pct = Some(v / 100.0)
                            }
                        }
                    } else if field.starts_with("gpukib=") {
                        if gpu_mem_gb.is_some() {
                            continue 'outer;
                        }
                        match f64::from_str(&field[7..]) {
                            Err(_) => {
                                println!("Failed gpukib");
                                continue 'outer;
                            }
                            Ok(v) => {
                                gpu_mem_gb = Some(v / (1024.0 * 1024.0))
                            }
                        }
                    } else {
                        // Unknown field, ignore it silently, this is benign.
                    }
                }

                // Check that mandatory fields are present.

                if version.is_none() || timestamp.is_none() || hostname.is_none() || user.is_none() ||
                    job_id.is_none() || command.is_none() || cpu_pct.is_none() || mem_gb.is_none()
                {
                    println!("Failed mandatory fields");
                    continue 'outer;
                }

                // Fill in default data for optional fields.

                if gpus.is_none() {
                    gpus = Some(Some(HashSet::new()))
                }
                if gpu_pct.is_none() {
                    gpu_pct = Some(0.0)
                }
                if gpu_mem_pct.is_none() {
                    gpu_mem_pct = Some(0.0)
                }
                if gpu_mem_gb.is_none() {
                    gpu_mem_gb = Some(0.0)
                }

                // Filter it

                if !include_record(&user.as_ref().unwrap(),
                                   &hostname.as_ref().unwrap(),
                                   job_id.unwrap(),
                                   &timestamp.unwrap()) {
                    println!("Failed filter");
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
                    gpu_mem_pct: gpu_mem_pct.unwrap(),
                    gpu_mem_gb: gpu_mem_gb.unwrap(),
                });
            }
        }
    }
    std::io::stdout().flush().unwrap();
    Ok(results)
}

#[cfg(test)]
fn filter(_user:&str, _host:&str, _job: u32, _t:&DateTime<Utc>) -> bool {
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
    let filter = |user:&str, _host:&str, _job: u32, _t:&DateTime<Utc>| {
        user == "riccarsi"
    };
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
    assert!(x[2].gpu_mask == Some(HashSet::from([4,5,6])));
}
