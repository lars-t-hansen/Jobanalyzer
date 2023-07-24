// Simple parser / preprocessor / input filterer for the Sonar log file format.
//
// Currently this handles the positional log file format only, where the fields are as described by
// the LogEntry struct, and in that order.

use anyhow::Result;
use chrono::prelude::DateTime;
use chrono::Utc;
use serde::Deserialize;
use crate::LogEntry;

/// Parse a log file into a set of LogEntry structures, applying an application-defined filter to
/// each record while reading.
///
/// This returns an error in the case of I/O errors, but silently drops records with parse errors.

pub fn parse_logfile<F>(
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

    // An error here is going to be an I/O error so always propagate it.
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(file_name)?;

    for deserialized_record in reader.deserialize::<LogRecord>() {
        match deserialized_record {
            Err(e) => {
                if e.is_io_error() {
                    return Err(e.into())
                }
                // Otherwise drop the record
            }
            Ok(record) => {
                match DateTime::parse_from_rfc3339(&record.timestamp) {
                    Err(_) => {
                        // Drop the record
                    }
                    Ok(t) => {
                        let timestamp : DateTime<Utc> = t.into();
	                if include_record(&record.user, &record.hostname, record.job_id, &timestamp) {
	                    match usize::from_str_radix(&record.gpu_mask, 2) {
                                Err(_) => {
                                    // Drop the record
                                }
                                Ok(gpu_mask) => {
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
                    }
                }
            }
        }
    }
    Ok(results)
}
