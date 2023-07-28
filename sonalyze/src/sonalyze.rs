// `sonalize` -- process Sonar log files and list jobs, with optional filtering and details.
//
// See MANUAL.md for a manual, or run with --help for brief help.

// TODO - High pri
//
// (Nothing)
//
//
// TODO - Normal pri
//
// (Also see TODOs in ../sonarlog/src)
//
// Bug: The time for `to` when a date yyyy-mm-dd is computed as yyyy-mm-ddT00:00:00 but the sensible
// value would be yyyy-mm-ddT23:59:59.
//
// Feature: Figure out how to show hosts / node names for a job.  (This is something that only
// matters when integrating with SLURM or other job queues, it can't be tested on the ML or
// light-HPC nodes.  So test on Fox.)  I think maybe an option --show-hosts would be appropriate,
// and in this case the list of hosts would be printed after the command?  Or instead of the
// command?
//
//
// TODO - Backlog / discussion
//
// Bug: For zombies, the "user name" can be longer than 8 chars and may need to be truncated or
// somehow managed, I think.  It's possible it shouldn't be printed if --zombie, but that's not
// the only case.
//
// Feature: Maybe `--at` as a way of specifying time, this would be a shorthand combining --from and
// --to with the same values, it is handy for selecting a specific day (but only that, and maybe too
// special purpose).  Perhaps a better feature is --duration, allowing eg --from=2w --duration=1w,
// or --from=yyyy-mm-dd --duration=1d.
//
// Feature: Useful feature would be "max peak" for gpu/vmem at least, to be more subtle than
// --no-gpu.  Alternatively, --little-gpu as a companion to --some-gpu.
//
// Feature: One could imagine other sort orders for the output than least-recently-started-first.
// This only matters for the --numjobs switch.
//
// Tweak: We allow for at most a two-digit number of days of running time in the output but in
// practice we're going to see some three-digit number of days, make room for that.
//
// Perf: Performance and memory use will become an issue with a large number of records?  Probably want to
// profile before we hack too much, but there are obvious inefficiencies in representations and the
// number of passes across data structures, and probably in the number of copies made (and thus the
// amount of memory allocation).
//
// Testing: Selftest cases everywhere, but esp for the argument parsers and filterers, and for the
// json reader.
//
//
// Quirks
//
// Having the absence of --user mean "only $LOGNAME" can be confusing -- though it's the right thing
// for a use case where somebody is looking only at her own jobs.
//
// The --from and --to values are used *both* for filtering files in the directory tree of logs
// (where it is used to generate directory names to search) *and* for filtering individual records
// in the log files.  Things can become a confusing if the log records do not have dates
// corresponding to the directories they are located in.  This is mostly a concern for testing;
// production data will have a sane mapping.
//
// Some filtering options select *records* (from, to, host, user, exclude) and some select *jobs*
// (the rest of them), and this can be confusing.  For user and exclude this does not matter (modulo
// setuid or similar personality changes).  The user might expect that from/to/host would select
// jobs instead of records, s.t. if a job ran in the time interval (had samples in the interval)
// then the entire job should be displayed, including data about it outside the interval.  Ditto,
// that if a job ran on a selected host then its work on all hosts should be displayed.  But it just
// ain't so.

mod configs;
mod jobs;
mod load;

use anyhow::{anyhow,bail,Result};
use chrono::prelude::{DateTime,NaiveDate};
use chrono::Utc;
use clap::{Args,Parser,Subcommand};
use sonarlog;
use std::collections::HashSet;
use std::env;
use std::num::ParseIntError;
use std::process;
use std::str::FromStr;
use std::time;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Print information about jobs
    Jobs(JobArgs),

    /// Print information about system load
    Load(LoadArgs),
}

#[derive(Args, Debug)]
pub struct JobArgs {
    #[command(flatten)]
    input_args: InputArgs,

    #[command(flatten)]
    filter_args: JobFilterArgs,

    #[command(flatten)]
    print_args: JobPrintArgs,

    #[command(flatten)]
    meta_args: MetaArgs,
}

#[derive(Args, Debug)]
pub struct LoadArgs {
    #[command(flatten)]
    input_args: InputArgs,

    #[command(flatten)]
    filter_args: LoadFilterArgs,

    #[command(flatten)]
    print_args: LoadPrintArgs,

    #[command(flatten)]
    meta_args: MetaArgs,
}

#[derive(Args, Debug)]
pub struct InputArgs {
    /// Select the root directory for log files [default: $SONAR_ROOT]
    #[arg(long)]
    data_path: Option<String>,

    /// Select these user name(s), comma-separated, "-" for all [default: command dependent]
    #[arg(long, short)]
    user: Option<String>,

    /// Exclude these user name(s), comma-separated [default: none]
    #[arg(long)]
    exclude: Option<String>,

    /// Select records with these job number(s), comma-separated [default: all]
    #[arg(long, short, value_parser = job_numbers)]
    job: Option<Vec<usize>>,
    
    /// Select records by this time and later.  Format can be YYYY-MM-DD, or Nd or Nw
    /// signifying N days or weeks ago [default: 1d, ie 1 day ago]
    #[arg(long, short, value_parser = parse_time)]
    from: Option<DateTime<Utc>>,

    /// Select records by this time and earlier.  Format can be YYYY-MM-DD, or Nd or Nw
    /// signifying N days or weeks ago [default: now]
    #[arg(long, short, value_parser = parse_time)]
    to: Option<DateTime<Utc>>,

    /// Select records and logs by these host name(s), comma-separated [default: all]
    #[arg(long)]
    host: Option<String>,

    /// Log file names (overrides --data-path)
    #[arg(last = true)]
    logfiles: Vec<String>,
}

#[derive(Args, Debug)]
pub struct LoadFilterArgs {
    /// Select records with this command name (case-sensitive substring) [default: all]
    #[arg(long)]
    command: Option<String>,

    /// Bucket and average records hourly, cf --daily and --none [default]
    #[arg(long)]
    hourly: bool,

    /// Bucket and average records daily
    #[arg(long)]
    daily: bool,

    /// Do not bucket and average records
    #[arg(long)]
    none: bool,
}

#[derive(Args, Debug)]
pub struct JobFilterArgs {
    /// Select jobs with this command name (case-sensitive substring) [default: all]
    #[arg(long)]
    command: Option<String>,

    /// Select only jobs with at least this many observations [default: 2]
    #[arg(long)]
    min_observations: Option<usize>,

    /// Select only jobs with at least this much average CPU use (100=1 full CPU)
    #[arg(long, default_value_t = 0)]
    min_avg_cpu: usize,

    /// Select only jobs with at least this much peak CPU use (100=1 full CPU)
    #[arg(long, default_value_t = 0)]
    min_peak_cpu: usize,

    /// Select only jobs with at least this much average main memory use (GB)
    #[arg(long, default_value_t = 0)]
    min_avg_mem: usize,

    /// Select only jobs with at least this much peak main memory use (GB)
    #[arg(long, default_value_t = 0)]
    min_peak_mem: usize, 

    /// Select only jobs with at least this much average GPU use (100=1 full GPU card)
    #[arg(long, default_value_t = 0)]
    min_avg_gpu: usize, 

    /// Select only jobs with at least this much peak GPU use (100=1 full GPU card)
    #[arg(long, default_value_t = 0)]
    min_peak_gpu: usize, 

    /// Select only jobs with at least this much average GPU memory use (100=1 full GPU card)
    #[arg(long, default_value_t = 0)]
    min_avg_vmem: usize, 

    /// Select only jobs with at least this much peak GPU memory use (100=1 full GPU card)
    #[arg(long, default_value_t = 0)]
    min_peak_vmem: usize, 

    /// Select only jobs with at least this much runtime, format `WwDdHhMm`, all parts optional [default: 0m]
    #[arg(long, value_parser = run_time)]
    min_runtime: Option<chrono::Duration>,

    /// Select only jobs with no GPU use
    #[arg(long, default_value_t = false)]
    no_gpu: bool,

    /// Select only jobs with some GPU use
    #[arg(long, default_value_t = false)]
    some_gpu: bool,

    /// Select only jobs that have run to completion
    #[arg(long, default_value_t = false)]
    completed: bool,

    /// Select only jobs that are still running
    #[arg(long, default_value_t = false)]
    running: bool,

    /// Select only zombie jobs (usually these are still running)
    #[arg(long, default_value_t = false)]
    zombie: bool,
}

#[derive(Args, Debug)]
pub struct LoadPrintArgs {
    /// Print records for all times (after bucketing), cf --last [default]
    #[arg(long)]
    all: bool,

    /// Print records for the last time instant (after bucketing)
    #[arg(long)]
    last: bool,

    /// Select fields for the output [default: datetime,cpu,mem,gpu,vmem,gpus]
    #[arg(long)]
    fmt: Option<String>,

    /// File containing JSON data with system information, for when we want to print system-relative values [default: none]
    #[arg(long)]
    config_file: Option<String>,
}

#[derive(Args, Debug)]
pub struct JobPrintArgs {
    /// Print at most these many most recent jobs per user [default: all]
    #[arg(long, short)]
    numjobs: Option<usize>,
}

#[derive(Args, Debug)]
pub struct MetaArgs {
    /// Print useful(?) statistics about the input and output
    #[arg(long, short, default_value_t = false)]
    verbose: bool,
    
    /// Print unformatted data (for developers)
    #[arg(long, default_value_t = false)]
    raw: bool,
}

// Comma-separated job numbers.
fn job_numbers(s: &str) -> Result<Vec<usize>> {
    let candidates = s.split(',').map(|x| usize::from_str(x)).collect::<Vec<Result<usize, ParseIntError>>>();
    if candidates.iter().all(|x| x.is_ok()) {
        Ok(candidates.iter().map(|x| *x.as_ref().unwrap()).collect::<Vec<usize>>())
    } else {
        bail!("Illegal job numbers: {s}")
    }
}

// YYYY-MM-DD, but with a little (too much?) flexibility.  Or Nd, Nw.
fn parse_time(s: &str) -> Result<DateTime<Utc>> {
    if let Some(n) = s.strip_suffix('d') {
        if let Ok(k) = usize::from_str(n) {
            Ok(Utc::now() - chrono::Duration::days(k as i64))
        } else {
            bail!("Invalid date: {s}")
        }
    } else if let Some(n) = s.strip_suffix('w') {
        if let Ok(k) = usize::from_str(n) {
            Ok(Utc::now() - chrono::Duration::weeks(k as i64))
        } else {
            bail!("Invalid date: {s}")
        }
    } else {
        let parts = s.split('-').map(|x| usize::from_str(x)).collect::<Vec<Result<usize, ParseIntError>>>();
        if !parts.iter().all(|x| x.is_ok()) || parts.len() != 3 {
            bail!("Invalid date syntax: {s}");
        }
        let vals = parts.iter().map(|x| *x.as_ref().unwrap()).collect::<Vec<usize>>();
        let d = NaiveDate::from_ymd_opt(vals[0] as i32, vals[1] as u32, vals[2] as u32);
        if !d.is_some() {
            bail!("Invalid date: {s}");
        }
        // See TODO item above, this is fine for `--from` but wrong for `--to`
        Ok(DateTime::from_utc(d.unwrap().and_hms_opt(0,0,0).unwrap(), Utc))
    }
}

// This is DdHhMm with all parts optional but at least one part required.  There is possibly too
// much flexibility here, as the parts can be in any order.
fn run_time(s: &str) -> Result<chrono::Duration> {
    let bad = anyhow!("Bad time duration syntax: {s}");
    let mut weeks = 0u64;
    let mut days = 0u64;
    let mut hours = 0u64;
    let mut minutes = 0u64;
    let mut have_weeks = false;
    let mut have_days = false;
    let mut have_hours = false;
    let mut have_minutes = false;
    let mut ds = "".to_string();
    for ch in s.chars() {
        if ch.is_digit(10) {
            ds = ds + &ch.to_string();
        } else {
            if ds == "" ||
                (ch != 'd' && ch != 'h' && ch != 'm' && ch != 'w') ||
                (ch == 'd' && have_days) || (ch == 'h' && have_hours) || (ch == 'm' && have_minutes) || (ch == 'w' && have_weeks) {
                    return Err(bad)
                }
            let v = u64::from_str(&ds);
            if !v.is_ok() {
                return Err(bad);
            }
            let val = v.unwrap();
            ds = "".to_string();
            if ch == 'd' {
                have_days = true;
                days = val;
            } else if ch == 'h' {
                have_hours = true;
                hours = val;
            } else if ch == 'm' {
                have_minutes = true;
                minutes = val;
            } else if ch == 'w' {
                have_weeks = true;
                weeks = val;
            }
        }
    }
    if ds != "" || (!have_days && !have_hours && !have_minutes && !have_weeks) {
        return Err(bad);
    }

    days += weeks * 7;
    hours += days * 24;
    minutes += hours * 60;
    let seconds = minutes * 60;
    match chrono::Duration::from_std(time::Duration::from_secs(seconds)) {
        Ok(e) => Ok(e),
        Err(_) => bail!("Bad running time")
    }
}

fn main() {
    match sonalyze() {
        Ok(()) => {}
        Err(msg) => {
            eprintln!("ERROR: {}", msg);
            process::exit(1);
        }
    }
}

fn sonalyze() -> Result<()> {
    let cli = Cli::parse();

    let input_args = match cli.command {
        Commands::Jobs(ref jobs_args) => &jobs_args.input_args,
        Commands::Load(ref load_args) => &load_args.input_args
    };

    let meta_args = match cli.command {
        Commands::Jobs(ref jobs_args) => &jobs_args.meta_args,
        Commands::Load(ref load_args) => &load_args.meta_args
    };

    // Validate and regularize input parameters from switches and defaults.

    let (from, to, include_hosts, include_jobs, include_users, exclude_users, logfiles) = {

        // Included date range.  These are used both for file names and for records.

        let from = if let Some(x) = input_args.from { x } else { Utc::now() - chrono::Duration::days(1) };
        let to = if let Some(x) = input_args.to { x } else { Utc::now() };
        if from > to {
            bail!("The --from time is greater than the --to time");
        }

        // Included host set.

        let include_hosts = if let Some(ref hosts) = input_args.host {
            let hosts = hosts.split(',').map(|x| x.to_string()).collect::<HashSet<String>>();
            if hosts.len() == 0 {
                bail!("At least one host for --host")
            }
            hosts
        } else {
            HashSet::new()
        };

        // Included job numbers.

        let include_jobs = if let Some(ref jobs) = input_args.job {
            let jobs = jobs.iter().map(|x| *x).collect::<HashSet<usize>>();
            if jobs.len() == 0 {
                bail!("At least one job for --job")
            }
            jobs
        } else {
            HashSet::new()
        };

        // Included users.  The default depends on some other switches.

        let all_users = {
            let is_load_cmd = if let Commands::Load(_) = cli.command {
                true
            } else {
                false
            };
            let only_zombie_jobs = if let Commands::Jobs(ref jobs_args) = cli.command {
                jobs_args.filter_args.zombie
            } else {
                false
            };
            is_load_cmd || only_zombie_jobs
        };

        let include_users = if let Some(ref users) = input_args.user {
            if users == "-" {
                HashSet::new()
            } else {
                let users = users.split(',').map(|x| x.to_string()).collect::<HashSet<String>>();
                if users.len() == 0 {
                    bail!("At least one user for --user")
                }
                users
            }
        } else if all_users {
            HashSet::new()
        } else {
            let mut users = HashSet::new();
            if let Ok(u) = env::var("LOGNAME") {
                users.insert(u);
            };
            users
        };

        // Excluded users.

        let mut exclude_users = if let Some(ref excl) = input_args.exclude {
            let excls = excl.split(',').map(|x| x.to_string()).collect::<HashSet<String>>();
            if excls.len() == 0 {
                bail!("At least one user for --exclude")
            }
            excls
        } else {
            HashSet::new()
        };
        exclude_users.insert("root".to_string());
        exclude_users.insert("zabbix".to_string());

        // Data path, if present.

        let data_path = if input_args.data_path.is_some() {
            input_args.data_path.clone()
        } else if let Ok(val) = env::var("SONAR_ROOT") {
            Some(val)
        } else if let Ok(val) = env::var("HOME") {
            Some(val + "/sonar_logs")
        } else {
            None
        };

        // Log files, filtered by host and time range.

        let logfiles =
            if input_args.logfiles.len() > 0 {
                input_args.logfiles.clone()
            } else {
                if meta_args.verbose {
                    eprintln!("Data path: {:?}", data_path);
                }
                if data_path.is_none() {
                    bail!("No data path");
                }
                let maybe_logfiles =
                    sonarlog::find_logfiles(&data_path.unwrap(), &include_hosts, from, to);
                if let Err(ref msg) = maybe_logfiles {
                    bail!("{msg}");
                }
                maybe_logfiles.unwrap()
            };

        if meta_args.verbose {
            eprintln!("Log files: {:?}", logfiles);
        }

        (from, to, include_hosts, include_jobs, include_users, exclude_users, logfiles)
    };

    // Input filtering logic is the same for both job and load listing, the only material
    // difference (handled above) is that the default user set for load listing is "all".

    let filter = |user:&str, host:&str, job: u32, t:&DateTime<Utc>| {
        ((&include_users).is_empty() || (&include_users).contains(user)) &&
            ((&include_hosts).is_empty() || (&include_hosts).contains(host)) &&
            ((&include_jobs).is_empty() || (&include_jobs).contains(&(job as usize))) &&
            !(&exclude_users).contains(user) &&
            from <= *t &&
            *t <= to
    };

    match cli.command {
        Commands::Load(ref load_args) => {
            let by_host = sonarlog::compute_load(&logfiles, &filter)?;
            load::aggregate_and_print_load(&include_hosts,
                                           &load_args.filter_args,
                                           &load_args.print_args,
                                           meta_args,
                                           &by_host)
        }
        Commands::Jobs(ref job_args) => {
            let (joblog, records_read, earliest, latest) = sonarlog::compute_jobs(&logfiles, &filter)?;
            if meta_args.verbose {
                eprintln!("Number of job records read: {}", records_read);
                eprintln!("Number of job records after input filtering: {}", joblog.len());
            }
            jobs::aggregate_and_print_jobs(&job_args.filter_args,
                                           &job_args.print_args,
                                           meta_args,
                                           joblog,
                                           earliest,
                                           latest)
        }
    }
}
