// `sonalize` -- Analyze `sonar` log files
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

use anyhow::{bail,Result};
use chrono::{Datelike,NaiveDate};
use clap::{Args,Parser,Subcommand};
use sonarlog::{self, HostFilter, Timestamp};
use std::collections::HashSet;
use std::env;
use std::num::ParseIntError;
use std::ops::Add;
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

    /// Include this user, "-" for all (repeatable) [default: command dependent]
    #[arg(long, short)]
    user: Vec<String>,

    /// Exclude this user (repeatable) [default: none]
    #[arg(long)]
    exclude: Vec<String>,

    /// Select this job (repeatable) [default: all]
    #[arg(long, short)]
    job: Vec<String>,
    
    /// Select records by this time and later.  Format can be YYYY-MM-DD, or Nd or Nw
    /// signifying N days or weeks ago [default: 1d, ie 1 day ago]
    #[arg(long, short, value_parser = parse_time_start_of_day)]
    from: Option<Timestamp>,

    /// Select records by this time and earlier.  Format can be YYYY-MM-DD, or Nd or Nw
    /// signifying N days or weeks ago [default: now]
    #[arg(long, short, value_parser = parse_time_end_of_day)]
    to: Option<Timestamp>,

    /// Select this host name (repeatable) [default: all]
    #[arg(long)]
    host: Vec<String>,

    /// File containing JSON data with system information, for when we want to print or use system-relative values [default: none]
    #[arg(long)]
    config_file: Option<String>,

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

    /// Select only jobs with at least this many samples [default: 2]
    #[arg(long)]
    min_samples: Option<usize>,

    /// Select only jobs with at least this much average CPU use (100=1 full CPU)
    #[arg(long, default_value_t = 0)]
    min_avg_cpu: usize,

    /// Select only jobs with at least this much peak CPU use (100=1 full CPU)
    #[arg(long, default_value_t = 0)]
    min_peak_cpu: usize,

    /// Select only jobs with at most this much average CPU use (100=1 full CPU)
    #[arg(long, default_value_t = 100000000)]
    max_avg_cpu: usize,

    /// Select only jobs with at most this much peak CPU use (100=1 full CPU)
    #[arg(long, default_value_t = 100000000)]
    max_peak_cpu: usize,

    /// Select only jobs with at least this much relative average CPU use (100=all cpus)
    #[arg(long, default_value_t = 0)]
    min_avg_rcpu: usize,

    /// Select only jobs with at least this much relative peak CPU use (100=all cpus)
    #[arg(long, default_value_t = 0)]
    min_peak_rcpu: usize,

    /// Select only jobs with at most this much relative average CPU use (100=all cpus)
    #[arg(long, default_value_t = 100)]
    max_avg_rcpu: usize,

    /// Select only jobs with at most this much relative peak CPU use (100=all cpus)
    #[arg(long, default_value_t = 100)]
    max_peak_rcpu: usize,

    /// Select only jobs with at least this much average main memory use (GB)
    #[arg(long, default_value_t = 0)]
    min_avg_mem: usize,

    /// Select only jobs with at least this much peak main memory use (GB)
    #[arg(long, default_value_t = 0)]
    min_peak_mem: usize, 

    /// Select only jobs with at least this much relative average main memory use (100=all memory)
    #[arg(long, default_value_t = 0)]
    min_avg_rmem: usize,

    /// Select only jobs with at least this much relative peak main memory use (100=all memory)
    #[arg(long, default_value_t = 0)]
    min_peak_rmem: usize,

    /// Select only jobs with at least this much average GPU use (100=1 full GPU card)
    #[arg(long, default_value_t = 0)]
    min_avg_gpu: usize, 

    /// Select only jobs with at least this much peak GPU use (100=1 full GPU card)
    #[arg(long, default_value_t = 0)]
    min_peak_gpu: usize, 

    /// Select only jobs with at most this much average GPU use (100=1 full GPU card)
    #[arg(long, default_value_t = 100000000)]
    max_avg_gpu: usize, 

    /// Select only jobs with at most this much peak GPU use (100=1 full GPU card)
    #[arg(long, default_value_t = 100000000)]
    max_peak_gpu: usize, 

    /// Select only jobs with at least this much relative average GPU use (100=all cards)
    #[arg(long, default_value_t = 0)]
    min_avg_rgpu: usize,

    /// Select only jobs with at least this much relative peak GPU use (100=all cards)
    #[arg(long, default_value_t = 0)]
    min_peak_rgpu: usize,

    /// Select only jobs with at most this much relative average GPU use (100=all cards)
    #[arg(long, default_value_t = 100)]
    max_avg_rgpu: usize,

    /// Select only jobs with at most this much relative peak GPU use (100=all cards)
    #[arg(long, default_value_t = 100)]
    max_peak_rgpu: usize,

    /// Select only jobs with at least this much average GPU memory use (100=1 full GPU card)
    #[arg(long, default_value_t = 0)]
    min_avg_gpumem: usize,

    /// Select only jobs with at least this much peak GPU memory use (100=1 full GPU card)
    #[arg(long, default_value_t = 0)]
    min_peak_gpumem: usize,

    /// Select only jobs with at least this much relative average GPU memory use (100=all cards)
    #[arg(long, default_value_t = 0)]
    min_avg_rgpumem: usize,

    /// Select only jobs with at least this much relative peak GPU memory use (100=all cards)
    #[arg(long, default_value_t = 0)]
    min_peak_rgpumem: usize,

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

    /// Select fields for the output [default: datetime,cpu,mem,gpu,gpumem,gpus]
    #[arg(long)]
    fmt: Option<String>,
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

// The command arg parsers don't need to include the string being parsed because the error generated
// by clap includes that.

// YYYY-MM-DD, but with a little (too much?) flexibility.  Or Nd, Nw.
fn parse_time(s: &str, end_of_day: bool) -> Result<Timestamp> {
    if let Some(n) = s.strip_suffix('d') {
        if let Ok(k) = usize::from_str(n) {
            Ok(sonarlog::now() - chrono::Duration::days(k as i64))
        } else {
            bail!("Invalid date")
        }
    } else if let Some(n) = s.strip_suffix('w') {
        if let Ok(k) = usize::from_str(n) {
            Ok(sonarlog::now() - chrono::Duration::weeks(k as i64))
        } else {
            bail!("Invalid date")
        }
    } else {
        let parts = s.split('-').map(|x| usize::from_str(x)).collect::<Vec<Result<usize, ParseIntError>>>();
        if !parts.iter().all(|x| x.is_ok()) || parts.len() != 3 {
            bail!("Invalid date syntax");
        }
        let vals = parts.iter().map(|x| *x.as_ref().unwrap()).collect::<Vec<usize>>();
        let d = NaiveDate::from_ymd_opt(vals[0] as i32, vals[1] as u32, vals[2] as u32);
        if !d.is_some() {
            bail!("Invalid date");
        }
        // TODO: This is roughly right, but what we want here is the day + 1 day and then use `<`
        // rather than `<=` in the filter.
        let (h,m,s) = if end_of_day { (23,59,59) } else { (0,0,0) };
        Ok(sonarlog::timestamp_from_ymdhms(d.unwrap().year(), d.unwrap().month(), d.unwrap().day(), h, m, s))
    }
}

fn parse_time_start_of_day(s: &str) -> Result<Timestamp> {
    parse_time(s, false)
}

fn parse_time_end_of_day(s: &str) -> Result<Timestamp> {
    parse_time(s, true)
}

// This is DdHhMm with all parts optional but at least one part required.  There is possibly too
// much flexibility here, as the parts can be in any order.
fn run_time(s: &str) -> Result<chrono::Duration> {
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
                    bail!("Bad suffix")
                }
            let v = u64::from_str(&ds);
            if !v.is_ok() {
                bail!("Bad number")
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
        bail!("Inconsistent")
    }

    days += weeks * 7;
    hours += days * 24;
    minutes += hours * 60;
    let seconds = minutes * 60;
    Ok(chrono::Duration::from_std(time::Duration::from_secs(seconds))?)
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

    let (from, to, include_hosts, include_jobs, include_users, exclude_users, system_config, logfiles) = {

        // Included date range.  These are used both for file names and for records.

        let from = if let Some(x) = input_args.from { x } else { sonarlog::now() - chrono::Duration::days(1) };
        let to = if let Some(x) = input_args.to { x } else { sonarlog::now() };
        if from > to {
            bail!("The --from time is greater than the --to time");
        }

        // Included host set, empty means "all"

        let include_hosts = {
            let mut hosts = HostFilter::new();
            for host in &input_args.host {
                hosts.insert(host)?;
            }
            hosts
        };

        // Included job numbers, empty means "all"

        let include_jobs = {
            let mut jobs = HashSet::<usize>::new();
            for job in &input_args.job {
                jobs.insert(usize::from_str(job)?);
            }
            jobs
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

        let include_users = {
            let mut users = HashSet::<String>::new();
            if input_args.user.len() > 0 {
                // Not the default value
                if input_args.user.iter().any(|user| user == "-") {
                    // Everyone, so do nothing
                } else {
                    for user in &input_args.user {
                        users.insert(user.to_string());
                    }
                }
            } else if all_users {
                // Everyone, so do nothing
            } else {
                if let Ok(u) = env::var("LOGNAME") {
                    users.insert(u);
                };
            }
            users
        };

        // Excluded users.

        let mut exclude_users = {
            let mut excluded = HashSet::<String>::new();
            if input_args.exclude.len() > 0 {
                // Not the default value
                for user in &input_args.exclude {
                    excluded.insert(user.to_string());
                }
            } else {
                // Nobody
            }
            excluded
        };
        exclude_users.insert("root".to_string());
        exclude_users.insert("zabbix".to_string());

        // Data path, if present.

        let data_path =
            if input_args.data_path.is_some() {
                input_args.data_path.clone()
            } else if let Ok(val) = env::var("SONAR_ROOT") {
                Some(val)
            } else if let Ok(val) = env::var("HOME") {
                Some(val + "/sonar_logs")
            } else {
                None
            };

        // System configuration, if specified.

        let system_config =
            if let Some(ref config_filename) = input_args.config_file {
                Some(configs::read_from_json(&config_filename)?)
            } else {
                None
            };

        // Log files, filtered by host and time range.
        //
        // If the log files are provided on the command line then there will be no filtering by host
        // name on the file name.  This is by design.

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

        (from, to, include_hosts, include_jobs, include_users, exclude_users, system_config, logfiles)
    };

    // Input filtering logic is the same for both job and load listing, the only material
    // difference (handled above) is that the default user set for load listing is "all".

    let filter = |user:&str, host:&str, job: u32, t:&Timestamp| {
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
            load::aggregate_and_print_load(&system_config,
                                           &include_hosts,
                                           &load_args.filter_args,
                                           &load_args.print_args,
                                           meta_args,
                                           &by_host)
        }
        Commands::Jobs(ref job_args) => {
            let (joblog, records_read, earliest, latest) =
                sonarlog::compute_jobs(&logfiles, &filter, /* merge_across_hosts= */ false)?;
            if meta_args.verbose {
                eprintln!("Number of samples read: {}", records_read);
                let numrec = joblog.iter().map(|(_, recs)| recs.len()).reduce(usize::add).unwrap_or_default();
                eprintln!("Number of samples after input filtering: {}", numrec);
                eprintln!("Number of jobs after input filtering: {}", joblog.len());
            }
            jobs::aggregate_and_print_jobs(&system_config,
                                           &job_args.filter_args,
                                           &job_args.print_args,
                                           meta_args,
                                           joblog,
                                           earliest,
                                           latest)
        }
    }
}
