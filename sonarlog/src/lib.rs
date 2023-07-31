// This library handles a tree of sonar log files.
//
// TODO (normal pri)
//
//  - The expectation is that we will add caching of parsed data at some point, that can be
//    transparent provided the caching is per-user and the user running the log processing has a
//    home directory and write access to it.
//
//  - This will transparently deal with old (untagged) and new (tagged) log file formats, and will
//    likely evolve to indicate, for each field (though possibly only for some fields), whether the
//    field is present in a record or not.
//
//  - Hostname filtering (beyond FQDN matching) must be implemented in logtree.md.
//
//  - The aggregate structure does not have fields for absolute vmem, and there are some things to
//    document re how vmem is exposed on various hardware

mod dates;
mod jobs;
mod load;
mod logfile;
mod logtree;

// Create a set of plausible log file names within a directory tree, for a date range and a set of
// included host files.

pub use logtree::find_logfiles;

// Parse a log file into a set of LogEntry structures, applying an application-defined filter to
// each record while reading.

pub use logfile::parse_logfile;

// Create a map from job ID to a vector of all the records for the job sorted ascending by
// timestamp, and return that map along with metadata about the unfiltered records.

pub use jobs::compute_jobs;

use chrono::prelude::DateTime;
use chrono::Utc;
use std::collections::HashSet;

/// The LogEntry structure holds slightly processed data from a log record: Percentages have been
/// normalized to the range [0.0,1.0], and memory sizes have been normalized to GB.

#[derive(Debug)]
pub struct LogEntry {
    pub version: String,
    pub timestamp: DateTime<Utc>,
    pub hostname: String,
    pub num_cores: u32,
    pub user: String,
    pub job_id: u32,
    pub command: String,
    pub cpu_pct: f64,
    pub mem_gb: f64,
    pub gpus: Option<HashSet<u32>>, // None for "none", empty set for "unknown", otherwise the precise set
    pub gpu_pct: f64,
    pub gpu_mem_pct: f64,
    pub gpu_mem_gb: f64,
}

/// Create a map from host name to a vector of maps from time stamp to all the records for that time,
/// return the maps sorted ascending by host name and time.

pub use load::compute_load;

