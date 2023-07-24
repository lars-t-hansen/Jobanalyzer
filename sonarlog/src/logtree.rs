// Enumerate log files in a log tree.

// For jobgraph, the log format is this:
//
//    let file_name = format!("{}/{}/{}/{}/{}.csv", data_path, year, month, day, hostname);
//
// where year is CE and month and day have leading zeroes if necessary, ie, these are split
// out from a standard ISO timestamp.
//
// We loop across dates in the tree below `data_path` and for each csv file, we check if it names an
// included host name.

use crate::dates;
use anyhow::{bail,Result};
use chrono::prelude::DateTime;
use chrono::Utc;
use itertools::Itertools;
use std::collections::HashSet;
use std::path;

/// Create a set of plausible log file names within a directory tree, for a date range and a set of
/// included host files.
//
/// `maybe_data_path` is the command line option, if present, with defaults applied (it may still be
/// None).  The files are filtered by the time range (always) and by the set of host names, if that
/// set is not empty.

pub fn find_logfiles(maybe_data_path: Option<String>,
                     hostnames: &HashSet<String>,
                     from: DateTime<Utc>,
                     to: DateTime<Utc>,
) -> Result<Vec<String>> {
    if maybe_data_path.is_none() {
        bail!("No viable log directory");
    }

    let path = maybe_data_path.unwrap();
    if !path::Path::new(&path).is_dir() {
        bail!("No viable log directory");
    }

    let logfiles = enumerate_log_files(&path, hostnames, from, to)?;
    if logfiles.len() == 0 {
        bail!("No log files found");
    }

    return Ok(logfiles);
}

fn enumerate_log_files(data_path: &str,
                       hostnames: &HashSet<String>,
                       from: DateTime<Utc>,
                       to: DateTime<Utc>
) -> Result<Vec<String>> {
    // Strings on the form YYYY-MM-DD
    let ds = dates::date_range(from, to);

    let mut filenames = vec![];
    for date in ds {
        let (year, month, day) = date.split('-').collect_tuple().expect("Internal error: Bad date");
        let dir_name = format!("{}/{}/{}/{}", data_path, year, month, day);
        let p = std::path::Path::new(&dir_name);
        if p.is_dir() {
            // TODO: Is it right to bail out here on error?  The directory could exist but not be
            // searchable, say.  Or it could have disappeared.
            let rd = p.read_dir()?;
            for entry in rd {
                if let Ok(entry) = entry {
                    let p = entry.path();
                    if let Some(ext) = p.extension() {
                        if ext == "csv" {
                            if !hostnames.is_empty() {
                                // Now filter the basename without the extension against the
                                // host names
                                if let Some(stem) = p.file_stem() {
                                    // TODO: to_str().unwrap() could fail here if not UTF8 path
                                    let stem = stem.to_str().unwrap();
                                    // TODO: The stem is usually some FQDN, but the hostnames may
                                    // contain just plain host names, eg, `ml8` and not
                                    // `ml8.hpc.uio.no`, because that's convenient for the user.  So
                                    // in this case, see if the stem's first element can be split
                                    // off too.
                                    if hostnames.contains(stem) {
                                        // TODO: to_str().unwrap() could fail here if not UTF8 path
                                        filenames.push(p.to_str().unwrap().to_string())
                                    }
                                }
                            } else {
                                // TODO: to_str().unwrap() could fail here if not UTF8 path
                                filenames.push(p.to_str().unwrap().to_string())
                            }
                        }
                    }
                } else {
                    // TODO: Bad directory entry is silently ignored for now
                }
            }
        }
    }
    Ok(filenames)
}

