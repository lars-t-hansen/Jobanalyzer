// Simple parser / preprocessor for the Sonar log file format.  This does only minimal processing,
// but it will do some filtering to reduce data volume.

use anyhow::Result;
use chrono::prelude::DateTime;
use chrono::Utc;
use serde::Deserialize;
use std::collections::HashSet;
use std::env;
use std::path;

#[derive(Debug)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub hostname: String,
    pub num_cores: u32,
    pub user: String,
    pub job_id: u32,
    pub command: String,
    pub cpu_pct: f64,
    pub mem_gb: f64,
    pub gpu_mask: usize,
    pub gpu_pct: f64,
    pub gpu_mem_pct: f64,
    pub gpu_mem_gb: f64,
}

// `maybe_logfiles` are files that've been requested specifically, if this is empty then none have
// been requested.  `data_path` is the command line option, if present.  The files are filtered by
// the time range (always) and by the set of host names, if not empty.

pub fn find_logfiles(maybe_logfiles: Vec<String>,
                     data_path: Option<String>,
                     hostnames: &HashSet<String>,
                     from: DateTime<Utc>,
                     to: DateTime<Utc>,
) -> Result<Vec<String>, String> {
    if maybe_logfiles.len() > 0 {
        return Ok(maybe_logfiles);
    }

    let path = if data_path.is_some() {
        data_path
    } else if let Ok(val) = env::var("SONAR_ROOT") {
        Some(val)
    } else if let Ok(val) = env::var("HOME") {
        Some(val + "/sonar_logs")
    } else {
        None
    };

    if path.is_none() {
        return Err("No viable log directory".to_string());
    }
    let path = path.unwrap();
    if !path::Path::new(&path).is_dir() {
        return Err("No viable log directory".to_string());
    }

    let logfiles = enumerate_log_files(&path, hostnames, from, to);
    if logfiles.len() == 0 {
        return Err("No log files found".to_string());
    }

    return Ok(logfiles);
}

// For jobgraph, the log format is this:
//    let file_name = format!("{}/{}/{}/{}/{}.csv", data_path, year, month, day, hostname);
// where we loop across dates and host names, and data_path defaults to /cluster/shared/sonar/data,
// akin to our SONAR_ROOT.
//
// Host names are a complication, plus host names are redundantly coded into the sonar output.  This
// allows log files to be catenated though, maybe just as well.

fn enumerate_log_files(root_dir: &str, hostnames: &HashSet<String>, from: DateTime<Utc>, to: DateTime<Utc>) -> Vec<String> {
    // FIXME
    vec![]
}

// Read entries from the log file and parse and filter them.

pub fn parse<F>(
    file_name: &str,
    include_record: F,
) -> Result<Vec<LogEntry>> where F: Fn(&str, &str, u32, &DateTime<Utc>) -> bool {

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
    if std::path::Path::new(&file_name).exists() {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_path(file_name)?;

        for record in reader.deserialize() {
            let record: LogRecord = record?;
            let timestamp : DateTime<Utc> =
                DateTime::parse_from_rfc3339(&record.timestamp)?.into();
	    if include_record(&record.user, &record.hostname, record.job_id, &timestamp) {
		let gpu_mask = usize::from_str_radix(&record.gpu_mask, 2)?;
		results.push(LogEntry {
                    timestamp,
                    hostname: record.hostname,
                    num_cores: record.num_cores,
                    user: record.user,
                    job_id: record.job_id,
                    command: record.command,
                    cpu_pct: record.cpu_percentage / 100.0,
                    mem_gb: (record.mem_kb as f64) / (1024.0 * 1024.0),
                    gpu_mask,
                    gpu_pct: record.gpu_percentage / 100.0,
                    gpu_mem_pct: record.gpu_mem_percentage / 100.0,
                    gpu_mem_gb: (record.gpu_mem_kb as f64) / (1024.0 * 1024.0),
		});
            }
        }
    }
    Ok(results)
}
