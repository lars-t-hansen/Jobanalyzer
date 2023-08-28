/// A sonar log is a structured log: Individual *log records* are structured such that data fields
/// can be found in them and extracted from them, and the various fields have specific and
/// documented meanings.  Log records are found in *log files*, which are in turn present in *log
/// trees* in the file system.
/// 
/// This library handles a log tree in various ways:
///
/// - It finds log files within the log tree, applying filters by date and host name.
///
/// - It parses the log records within the log files, handling both the older record format (no
///   fields names) and the newer record format (field names) transparently.
///
/// - It cleans up and filters the log data if asked to do so.
///
/// - It abstracts some log data types (timestamps, GPU sets) in useful ways.
///
/// (Support for older field names is now opt-in under the feature "untagged-sonar-data".)

mod dates;
mod hosts;
mod jobs;
mod load;
mod logclean;
mod logfile;
mod logtree;
mod pattern;

// Types and utilities for manipulating timestamps.

pub use dates::Timestamp;

// "A long long time ago".

pub use dates::epoch;

// The time right now.

pub use dates::now;

// Parse a &str into a Timestamp.

pub use dates::parse_timestamp;

// Given year, month, day, hour, minute, second (all UTC), return a Timestamp.

pub use dates::timestamp_from_ymdhms;

// Given year, month, day (all UTC), return a Timestamp.

pub use dates::timestamp_from_ymd;

// Return the timestamp with minutes and seconds cleared out.

pub use dates::truncate_to_hour;

// Return the timestamp with hours, minutes, and seconds cleared out.

pub use dates::truncate_to_day;

// Compute a set of plausible log file names within a directory tree, for a date range and a set of
// included host names.

pub use logtree::find_logfiles;

// Read a set of logfiles into a vector and compute some simple metadata.

pub use logtree::read_logfiles;

// Parse a log file into a set of LogEntry structures, applying an application-defined filter to
// each record while reading.

pub use logfile::parse_logfile;

// A GpuSet is None, Some({}), or Some({a,b,...}), representing unknown, empty, or non-empty.

pub use logfile::GpuSet;

// Create an empty GpuSet.

pub use logfile::empty_gpuset;

// Create a GpuSet that is either None or Some({a}), depending on input.

pub use logfile::singleton_gpuset;

// Union one GpuSet into another (destructively).

pub use logfile::union_gpuset;

// Postprocess a vector of log data: compute the cpu_util_pct field and apply a record filter.

pub use logclean::postprocess_log;

/// The LogEntry structure holds slightly processed data from a log record: Percentages have been
/// normalized to the range [0.0,1.0] (except that the CPU and GPU percentages are sums across
/// multiple cores/cards and the sums may exceed 1.0), and memory sizes have been normalized to GB.
///
/// Any discrepancies between the documentation in this structure and the documentation for Sonar
/// (in its top-level README.md) should be considered a bug.

#[derive(Debug)]
pub struct LogEntry {
    /// Format "major.minor.bugfix"
    pub version: String,

    /// The time is common to all records created by the same sonar invocation.  It has no subsecond
    /// precision.
    pub timestamp: Timestamp,

    /// Fully qualified domain name.
    pub hostname: String,

    /// Number of cores on the node.  This may be zero if there's no information.
    pub num_cores: u32,

    /// Unix user name, or `_zombie_<PID>`
    pub user: String,

    /// For a unique process, this is its pid.  For a rolled-up job record with multiple processes,
    /// this is initially zero, but logclean converts it to job_id + 10000000.
    pub pid: u32,

    /// The job_id.  This has some complicated constraints, see the Sonar docs.
    pub job_id: u32,

    /// The command contains at least the executable name.  It may contain spaces and other special
    /// characters.  This can be `_unknown_` for zombie jobs and `_noinfo_` for non-zombie jobs when
    /// the command can't be found.
    pub command: String,

    /// This is a running average of the CPU usage of the job, over the lifetime of the job, summed
    /// across all the processes of the job.  IT IS NOT A SAMPLE.  100.0=1 core's worth (100%).
    /// Generally, `cpu_util_pct` (below) will be more useful.
    pub cpu_pct: f64,

    /// Main memory used by the job on the node (the memory is shared by all cores on the node) at
    /// the time of sampling.
    pub mem_gb: f64,

    /// The set of GPUs used by the job on the node, None for "none", Some({}) for "unknown",
    /// otherwise Some({m,n,...}).
    pub gpus: GpuSet,

    /// Percent of the sum of the capacity of all GPUs in `gpus`.  100.0 means 1 card's worth of
    /// compute (100%).  This value may be larger than 100.0 as it's the sum across cards.
    ///
    /// For NVIDIA, this is utilization since the last sample.  (nvidia-smi pmon -c 1 -s mu).
    /// For AMD, this is instantaneous utilization (rocm-smi or rocm-smi -u)
    pub gpu_pct: f64,

    /// GPU memory used by the job on the node at the time of sampling, as a percentage of all the
    /// memory on all the cards in `gpus`.  100.0 means 1 card's worth of memory (100%).  This value
    /// may be larger than 100.0 as it's the sum across cards.
    ///
    /// Note this is not always reliable in its raw form.  See Sonar documentation.
    pub gpumem_pct: f64,

    /// GPU memory used by the job on the node at the time of sampling, naturally across all GPUs in
    /// `gpus`.
    ///
    /// Note this is not always reliable in its raw form.  See Sonar documentation.
    pub gpumem_gb: f64,

    /// Accumulated CPU time for the process since the start, including time for any of its children
    /// that have terminated.
    pub cputime_sec: f64,

    /// Number of *other* processes (with the same host and command name) that were rolled up into
    /// this process record.
    pub rolledup: u32,

    // Computed fields

    /// CPU utilization in percent (100% = one full core) in the time interval since the previous
    /// record for this job.  This is computed by logclean from consecutive `cputime_sec` fields for
    /// records that represent the same job, when the information is available: it requires the
    /// `pid` and `cputime_sec` fields to be meaningful.
    pub cpu_util_pct: f64,
}

// A datum representing a key in the jobs map: (host-name, job-id).

pub use jobs::JobKey;

// Create a map from JobKey to a vector of all the records for the job sorted ascending by
// timestamp, and return that map along with metadata about the unfiltered records.

pub use jobs::compute_jobs;

// Create a map from host name to a vector of maps from time stamp to all the records for that time,
// return the maps sorted ascending by host name and time.

pub use load::compute_load;

// Structure representing a host name filter: basically a restricted automaton matching host names
// in useful ways.

pub use hosts::HostFilter;
