// Simple parser / preprocessor for the Sonar log file format.  This does only minimal processing,
// but it will do some filtering to reduce data volume.

use anyhow::Result;
use chrono::prelude::DateTime;
use chrono::Utc;
use serde::Deserialize;

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

// Read entries from the log file and parse them, keeping the ones for
// which include_record() return true.

pub fn parse<F>(
    file_name: &str,
    include_record: F,
) -> Result<Vec<LogEntry>> where F: Fn(&str, &DateTime<Utc>) -> bool {

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
	    if include_record(&record.user, &timestamp) {
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
