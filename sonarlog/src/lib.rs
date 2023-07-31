// This library handles a tree of sonar log files.  It finds files and parses them.  It can handle
// older (no fields names) and newer (field names) transparently.
//
// TODO (normal pri)
//
//  - The expectation is that we will add caching of parsed data at some point, that can be
//    transparent provided the caching is per-user and the user running the log processing has a
//    home directory and write access to it.
//
//  - Hostname filtering (beyond FQDN matching) must be implemented in logtree.md.

mod dates;
mod jobs;
mod load;
mod logfile;
mod logtree;

use chrono::prelude::DateTime;
use chrono::Utc;
use std::collections::HashSet;

// Compute a set of plausible log file names within a directory tree, for a date range and a set of
// included host names.

pub use logtree::find_logfiles;

// Parse a log file into a set of LogEntry structures, applying an application-defined filter to
// each record while reading.

pub use logfile::parse_logfile;

/// The LogEntry structure holds slightly processed data from a log record: Percentages have been
/// normalized to the range [0.0,1.0], and memory sizes have been normalized to GB.

#[derive(Debug)]
pub struct LogEntry {
    /// Format "major.minor.bugfix"
    pub version: String,

    /// The time is common to all records created by the same sonar invocation.  It has no subsecond
    /// precision.
    pub timestamp: DateTime<Utc>,

    /// Fully qualified domain name.
    pub hostname: String,
    
    /// Number of cores on the node.  This is never zero.
    pub num_cores: u32,

    /// Unix user name, or "_zombie_something" or "_unknown_"
    pub user: String,

    /// The job_id is ideally never zero, but sometimes it will be if no job ID can be computed.
    pub job_id: u32,

    /// The command contains at least the executable name.  It may contain spaces and other special
    /// characters.
    pub command: String,

    /// For CPU usage, 1.0 means 1 full core's worth.
    pub cpu_pct: f64,

    /// Main memory used by the job on the node (the memory is shared by all cores on the node).
    pub mem_gb: f64,

    /// The set of GPUs used by the job on the node, None for "none", Some({}) for "unknown",
    /// otherwise Some({m,n,...}).
    pub gpus: Option<HashSet<u32>>,

    /// Percent of the sum of the capacity of all GPUs in `gpus`.
    pub gpu_pct: f64,

    /// Percent of the sum of the capacity of all GPUs in `gpus`.  Note this is not always
    /// reliable.
    pub gpu_mem_pct: f64,

    /// Memory usage across all GPUs in `gpus`.  Note this is not always reliable.
    pub gpu_mem_gb: f64,
}

// Create a map from job ID to a vector of all the records for the job sorted ascending by
// timestamp, and return that map along with metadata about the unfiltered records.

pub use jobs::compute_jobs;

// Create a map from host name to a vector of maps from time stamp to all the records for that time,
// return the maps sorted ascending by host name and time.

pub use load::compute_load;

