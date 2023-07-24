// This library handles a tree of sonar log files.
//
// TODO:
//
//  - The expectation is that we will add caching of parsed
//
//  - This will transparently deal with old (untagged) and new (tagged) log file formats, and will
//    likely evolve to indicate, for each field (though possibly only for some fields), whether the
//    field is present in a record or not.

mod dates;
mod logfile;
mod logtree;

// Create a set of plausible log file names within a directory tree, for a date range and a set of
// included host files.

pub use logtree::find_logfiles;

// Parse a log file into a set of LogEntry structures, applying an application-defined filter to
// each record while reading.

pub use logfile::parse_logfile;

use chrono::prelude::DateTime;
use chrono::Utc;

/// The LogEntry structure holds slightly processed data from a log record: Percentages have been
/// normalized to the range [0.0,1.0], and memory sizes have been normalized to GB.

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
