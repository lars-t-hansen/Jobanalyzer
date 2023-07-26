// `lsjobs` -- process Sonar log files and list jobs, with optional filtering and details.
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
// Figure out how to show hosts / node names for a job.  (This is something that only matters when
// integrating with SLURM or other job queues, it can't be tested on the ML or light-HPC nodes.  So
// test on Fox.)  I think maybe an option --show-hosts would be appropriate, and in this case the
// list of hosts would be printed after the command?  Or instead of the command?
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
// special purpose).
//
// Feature ("manual monitoring" use case): Figure out how to show load.
//
//   Definition: the "load at time t on a host" is the sum across all jobs at time t of
//   cpu/gpu/mem/vmem, with the same meanings as those fields have.  (This can then be related to
//   the configuration of that host but that's for later.)
//
//   This presupposes that the sonar log uses the same time stamp for all records captured at a
//   given time (it currently does this) or that we establish a time window for observations that
//   are to be summed.  For now, there's no reason to establish such a time window.
//
//   The "historical load" of a host is then a table of the load at times through history, computed
//   every time sonar has a sample for the host.
//
//   There is a complication if we want the *printed* historical load to be extracted from the full
//   table; for example, if we sample every five minutes but want to print the load hourly.  In this
//   case, some kind of average of the load values over a time period would be the printed load.
//
//   Thus we have --load=<something> which specifies how to compute and display the load.  This
//   implies --user=- instead of --user=$LOGNAME (if not specified) and requires --host=<hostname> (but why?).
//
//   The <something> specifies what to print: `last` implies the last sample time; `all` is the
//   full log for the time window; `hourly` and `daily` are averages within the time window.
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
// Testing: Selftest cases everywhere, but esp for the argument parsers and filterers.
//
// Structure: Maybe refactor the argument processing into a separate file, it's becoming complex
// enough.  Wait until output filtering logic is in order.
//
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
// corresponding to the directories they are located in.  This is mostly a concern for testing.
//
// Some filtering options select *records* (from, to, host, user, exclude) and some select *jobs*
// (the rest of them), and this can be confusing.  For user and exclude this does not matter (modulo
// setuid or similar personality changes).  The user might expect that from/to/host would select
// jobs instead of records, s.t. if a job ran in the time interval (had samples in the interval)
// then the entire job should be displayed, including data about it outside the interval.  Ditto,
// that if a job ran on a selected host then its work on all hosts should be displayed.  But it just
// ain't so.

mod jobs;
mod load;

use chrono::prelude::{DateTime,NaiveDate};
use chrono::Utc;
use clap::Parser;
use sonarlog;
use std::collections::HashSet;
use std::env;
use std::num::ParseIntError;
use std::process;
use std::str::FromStr;
use std::time;

use load::{LoadFmt,aggregate_and_print_load};
use jobs::aggregate_and_print_jobs;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Select the root directory for log files [default: $SONAR_ROOT]
    #[arg(long)]
    data_path: Option<String>,

    /// Select these user name(s), comma-separated, "-" for all [default: $LOGNAME for job listing; all for load listing]
    #[arg(long, short)]
    user: Option<String>,

    /// Exclude these user name(s), comma-separated [default: none]
    #[arg(long)]
    exclude: Option<String>,

    /// Select these job number(s), comma-separated [default: all]
    #[arg(long, short, value_parser = job_numbers)]
    job: Option<Vec<usize>>,
    
    /// Select only jobs with this command name (case-sensitive substring) [default: all]
    #[arg(long)]
    command: Option<String>,

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

    /// Select only jobs with at least this many observations [default: 2 for job listing; illegal for load listing]
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

    /// Print system load instead of jobs, argument is `last`,`hourly`,`daily` [default: none]
    #[arg(long)]
    load: Option<String>,

    /// Print at most these many most recent jobs per user [default: all for job listing; illegal for load listing]
    #[arg(long, short)]
    numjobs: Option<usize>,

    /// Print useful(?) statistics about the input and output
    #[arg(long, short, default_value_t = false)]
    verbose: bool,
    
    /// Print unformatted data (for developers)
    #[arg(long, default_value_t = false)]
    raw: bool,

    /// Log file names (overrides --data-path)
    #[arg(last = true)]
    logfiles: Vec<String>,
}

// Comma-separated job numbers.
fn job_numbers(s: &str) -> Result<Vec<usize>, String> {
    let candidates = s.split(',').map(|x| usize::from_str(x)).collect::<Vec<Result<usize, ParseIntError>>>();
    if candidates.iter().all(|x| x.is_ok()) {
        Ok(candidates.iter().map(|x| *x.as_ref().unwrap()).collect::<Vec<usize>>())
    } else {
        Err("Illegal job numbers: ".to_string() + s)
    }
}

// YYYY-MM-DD, but with a little (too much?) flexibility.  Or Nd, Nw.
fn parse_time(s: &str) -> Result<DateTime<Utc>, String> {
    if let Some(n) = s.strip_suffix('d') {
        if let Ok(k) = usize::from_str(n) {
            Ok(now() - chrono::Duration::days(k as i64))
        } else {
            Err(format!("Invalid date: {}", s))
        }
    } else if let Some(n) = s.strip_suffix('w') {
        if let Ok(k) = usize::from_str(n) {
            Ok(now() - chrono::Duration::weeks(k as i64))
        } else {
            Err(format!("Invalid date: {}", s))
        }
    } else {
        let parts = s.split('-').map(|x| usize::from_str(x)).collect::<Vec<Result<usize, ParseIntError>>>();
        if !parts.iter().all(|x| x.is_ok()) || parts.len() != 3 {
            return Err(format!("Invalid date syntax: {}", s));
        }
        let vals = parts.iter().map(|x| *x.as_ref().unwrap()).collect::<Vec<usize>>();
        let d = NaiveDate::from_ymd_opt(vals[0] as i32, vals[1] as u32, vals[2] as u32);
        if !d.is_some() {
            return Err(format!("Invalid date: {}", s));
        }
        // See TODO item above, this is fine for `--from` but wrong for `--to`
        Ok(DateTime::from_utc(d.unwrap().and_hms_opt(0,0,0).unwrap(), Utc))
    }
}

// This is DdHhMm with all parts optional but at least one part required.  There is possibly too
// much flexibility here, as the parts can be in any order.
fn run_time(s: &str) -> Result<chrono::Duration, String> {
    let bad = format!("Bad time duration syntax: {}", s);
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
        Err(_) => Err("Bad running time".to_string())
    }
}

fn main() {
    let mut cli = Cli::parse();

    // Perform some ad-hoc validation.

    if let Some(ref l) = cli.load {
        match l.as_str() {
            "all" | "last" | "hourly" | "daily" => {},
            _ => fail("--load requires a value `all`, `last`, `hourly`, `daily`")
        }
    }

    // Figure out the data path from switches and defaults.

    let data_path = if cli.data_path.is_some() {
        cli.data_path.clone()
    } else if let Ok(val) = env::var("SONAR_ROOT") {
        Some(val)
    } else if let Ok(val) = env::var("HOME") {
        Some(val + "/sonar_logs")
    } else {
        None
    };

    // Convert the input filtering options to a useful form.

    let from = if let Some(x) = cli.from { x } else { one_day_ago() };
    let to = if let Some(x) = cli.to { x } else { now() };
    if from > to {
        fail("The --from time is greater than the --to time");
    }

    let include_hosts = if let Some(ref hosts) = cli.host {
        hosts.split(',').map(|x| x.to_string()).collect::<HashSet<String>>()
    } else {
        HashSet::new()
    };

    let include_jobs = if let Some(ref jobs) = cli.job {
        jobs.iter().map(|x| *x).collect::<HashSet<usize>>()
    } else {
        HashSet::new()
    };

    let include_users = if let Some(ref users) = cli.user {
        if users == "-" {
            HashSet::new()
        } else {
            users.split(',').map(|x| x.to_string()).collect::<HashSet<String>>()
        }
    } else if cli.zombie || cli.load.is_some() {
        HashSet::new()
    } else {
        let mut users = HashSet::new();
        if let Ok(u) = env::var("LOGNAME") {
            users.insert(u);
        };
        users
    };

    let mut exclude_users = if let Some(ref excl) = cli.exclude {
        excl.split(',').map(|x| x.to_string()).collect::<HashSet<String>>()
    } else {
        HashSet::new()
    };
    exclude_users.insert("root".to_string());
    exclude_users.insert("zabbix".to_string());

    if cli.load.is_some() {
        if cli.min_observations.is_some() {
            eprintln!("ERROR: --min-observations is not legal with --load");
            return;
        }
        if cli.numjobs.is_some() {
            eprintln!("ERROR: --numjobs is not legal with --load");
            return;
        }
    }

    // Logfiles, filtered by host and time range.

    let logfiles =
        if cli.logfiles.len() > 0 {
            cli.logfiles.split_off(0)
        } else {
            if cli.verbose {
                eprintln!("Data path: {:?}", data_path);
            }
            if data_path.is_none() {
                eprintln!("ERROR: No data path");
                return;
            }
            let maybe_logfiles = sonarlog::find_logfiles(&data_path.unwrap(), &include_hosts, from, to);
            if let Err(ref msg) = maybe_logfiles {
                fail(&format!("{}", msg));
            }
            maybe_logfiles.unwrap()
        };

    if cli.verbose {
        eprintln!("Log files: {:?}", logfiles);
    }

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

    if let Some(which_listing) = cli.load {
        match sonarlog::compute_load(&logfiles, &filter) {
            Ok(by_host) => {
                // TODO: Only a default listing, for now.  Need to implement a switch to control the format.
                let full = vec![LoadFmt::DateTime,LoadFmt::CpuPct,LoadFmt::MemGB,LoadFmt::GpuPct,LoadFmt::VmemGB,LoadFmt::VmemPct,LoadFmt::GpuMask];
                aggregate_and_print_load(&by_host, &which_listing, &full, cli.verbose);
            }
            Err(e) => {
                eprintln!("ERROR: {:?}", e);
                return;
            }
        }
    } else {
        match sonarlog::compute_jobs(&logfiles, &filter) {
            Ok((joblog, records_read, earliest, latest)) => {
                if cli.verbose {
                    eprintln!("Number of job records read: {}", records_read);
                    eprintln!("Number of job records after input filtering: {}", joblog.len());
                }
                aggregate_and_print_jobs(cli, joblog, earliest, latest);
            }
            Err(e) => {
                eprintln!("ERROR: {:?}", e);
                return;
            }
        }
    }
}

fn now() -> DateTime<Utc> {
    Utc::now()
}

fn one_day_ago() -> DateTime<Utc> {
    now() - chrono::Duration::days(1)
}

fn fail(msg: &str) {
    eprintln!("ERROR: {}", msg);
    process::exit(1);
}
