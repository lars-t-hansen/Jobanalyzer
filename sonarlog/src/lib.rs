/// This library handles a tree of sonar log files.  It finds files and parses them.  It can handle
/// the older format (no fields names) and the newer format (field names) transparently.
// TODO (normal pri)
//
//  - The expectation is that we will add caching of parsed data at some point, that can be
//    transparent provided the caching is per-user and the user running the log processing has a
//    home directory and write access to it.
mod dates;
mod hosts;
mod jobs;
mod load;
mod logfile;
mod logtree;
mod pattern;

use std::collections::HashSet;

// Types and utilities for manipulating timestamps.

pub use dates::Timestamp;

// "A long long time ago"

pub use dates::epoch;

// The time right now

pub use dates::now;

// Parse a &str into a Timestamp

pub use dates::parse_timestamp;

// Given year, month, day, hour, minute, second (all UTC), return a Timestamp

pub use dates::timestamp_from_ymdhms;

// Given year, month, day (all UTC), return a Timestamp

pub use dates::timestamp_from_ymd;

// Return the timestamp with minutes and seconds cleared out.

pub use dates::truncate_to_hour;

// Return the timestamp with hours, minutes, and seconds cleared out.

pub use dates::truncate_to_day;

// Compute a set of plausible log file names within a directory tree, for a date range and a set of
// included host names.

pub use logtree::find_logfiles;

// Parse a log file into a set of LogEntry structures, applying an application-defined filter to
// each record while reading.

pub use logfile::parse_logfile;

/// The LogEntry structure holds slightly processed data from a log record: Percentages have been
/// normalized to the range [0.0,1.0] (except that the GPU percentages are sums across multiple
/// cards and the sums may exceed 1.0), and memory sizes have been normalized to GB.

#[derive(Debug)]
pub struct LogEntry {
    /// Format "major.minor.bugfix"
    pub version: String,

    /// The time is common to all records created by the same sonar invocation.  It has no subsecond
    /// precision.
    pub timestamp: Timestamp,

    /// Fully qualified domain name.
    pub hostname: String,

    /// Number of cores on the node.  This is never zero.
    pub num_cores: u32,

    /// Unix user name, or "_zombie_something" or "_unknown_".
    pub user: String,

    /// The job_id is ideally never zero, but sometimes it will be if no job ID can be computed.
    pub job_id: u32,

    /// The command contains at least the executable name.  It may contain spaces and other special
    /// characters.
    pub command: String,

    /// For CPU usage, 100.0 means 1 full core's worth (100%).
    pub cpu_pct: f64,

    /// Main memory used by the job on the node (the memory is shared by all cores on the node).
    pub mem_gb: f64,

    /// The set of GPUs used by the job on the node, None for "none", Some({}) for "unknown",
    /// otherwise Some({m,n,...}).
    pub gpus: Option<HashSet<u32>>,

    /// Percent of the sum of the capacity of all GPUs in `gpus`.  100.0 means 1 card's worth of
    /// compute (100%).  This value may be larger than 100.0 as it's the sum across cards.
    pub gpu_pct: f64,

    /// Percent of the sum of the capacity of all GPUs in `gpus`.  (Note this is not always
    /// reliable.)  100.0 means 1 card's worth of memory (100%).  This value may be larger than
    /// 100.0 as it's the sum across cards.
    pub gpumem_pct: f64,

    /// Memory usage across all GPUs in `gpus`.  (Note this is not always reliable.)
    pub gpumem_gb: f64,
}

// A datum representing a complex key in the jobs map, using just the job ID or the (job ID, host
// name).  Construct with JobKey::from_entry and JobKey::from_parts.

pub use jobs::JobKey;

// Create a map from JobKey to a vector of all the records for the job sorted ascending by
// timestamp, and return that map along with metadata about the unfiltered records.

pub use jobs::compute_jobs;

// Create a map from host name to a vector of maps from time stamp to all the records for that time,
// return the maps sorted ascending by host name and time.

pub use load::compute_load;

// Structure representing a host name filter.

pub use hosts::HostFilter;
