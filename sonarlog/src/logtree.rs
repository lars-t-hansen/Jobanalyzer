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
use anyhow::{bail, Result};
use chrono::prelude::DateTime;
use chrono::Utc;
use std::collections::HashSet;
use std::path;

/// Create a set of plausible log file names within a directory tree, for a date range and a set of
/// included host files.
///
/// `data_path` is the root of the tree.  `hostnames`, if not the empty set, is the set of hostnames
/// we will want data for.  `from` and `to` express the inclusive date range for the records we will
/// want to consider.
///
/// This returns an error if the `data_path` does not name a directory or if any directory that is
/// considered in the subtree, and which exists, cannot be read.
///
/// It does not return an error if the csv files cannot be read; that has to be handled later.
///
/// File names that are not representable as UTF8 are ignored.

pub fn find_logfiles(
    data_path: String,
    hostnames: &HashSet<String>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<String>> {
    if !path::Path::new(&data_path).is_dir() {
        bail!("No viable log directory");
    }

    let mut filenames = vec![];
    for (year, month, day) in dates::date_range(from, to) {
        let dir_name = format!("{}/{}/{:02}/{:02}", data_path, year, month, day);
        let p = std::path::Path::new(&dir_name);
        if p.is_dir() {
            let rd = p.read_dir()?;
            for entry in rd {
                if let Err(_) = entry {
                    // Bad directory entries are ignored
                    continue
                }
                let p = entry.unwrap().path();
                let pstr = p.to_str();
                if pstr.is_none() {
                    // Non-UTF8 paths are ignored.  The `data_path` is a string, hence UTF8, and we
                    // construct only UTF8 names, and host names are UTF8.  Hence non-UTF8 names
                    // will never match what we're looking for.
                    continue
                }
                let ext = p.extension();
                if ext.is_none() || ext.unwrap() != "csv" {
                    // Non-csv files are ignored
                    continue
                }
                if hostnames.is_empty() {
                    // If there's no hostname filter then keep the path
                    filenames.push(pstr.unwrap().to_string());
                    continue
                }
                let h = p.file_stem();
                if h.is_none() {
                    // File names have to have a stem even if there is no host name filter.
                    continue
                }
                let stem = h.unwrap().to_str().unwrap();
                // Filter the stem against the host names.
                //
                // TODO: The stem is usually some FQDN, but `hostnames` may contain just plain host
                // names, eg, `ml8` and not `ml8.hpc.uio.no`, because that's convenient for the
                // user.  So in this case, see if the stem's first element can be split off too.
                if hostnames.contains(stem) {
                    filenames.push(pstr.unwrap().to_string());
                    continue
                }
            }
        }
    }
    Ok(filenames)
}
