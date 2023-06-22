use anyhow::Result;
use chrono::prelude::DateTime;
use chrono::Utc;
use serde::Deserialize;
use std::collections::HashSet;

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

// Read entries from the log and parse them, keeping the ones for the user (or for all the users if
// users==None) in the date range (if supplied).

pub fn parse(
    file_name: &str,
    users: Option<&HashSet<String>>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
) -> Result<Vec<LogEntry>> {
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
    let error_message = "INTERNAL ERROR in lsjobs".to_string();
    if std::path::Path::new(&file_name).exists() {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_path(file_name)
            .expect(&error_message);

        for record in reader.deserialize() {
            let record: LogRecord = record.expect(&error_message);
            if users.is_none() || users.unwrap().contains(&record.user) {
                let timestamp : DateTime<Utc> =
                    DateTime::parse_from_rfc3339(&record.timestamp).expect(&error_message).into();
                if (from.is_none() || from.unwrap() <= timestamp) &&
                    (to.is_none() || timestamp <= to.unwrap()) {
                        let gpu_mask =
                            usize::from_str_radix(&record.gpu_mask, 2).expect(&error_message);
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
    Ok(results)
}
