// Generic formatting code for a set of data extracted from a data structure to be presented
// columnar or as csv, with or without a header.

use std::collections::{HashMap, HashSet};
use std::io;

/// Return a vector of the known fields in `spec` wrt the formatters, and a HashSet of any other
/// strings found in `spec`

pub fn parse_fields<'a, DataT, FmtT, CtxT>(
    spec: &'a str,
    formatters: &HashMap<String, FmtT>) -> (Vec<&'a str>, HashSet<&'a str>)
where
    FmtT: Fn(&DataT, CtxT) -> String,
    CtxT: Copy
{
    let mut others = HashSet::new();
    let mut fields = vec![];
    for x in spec.split(',') {
        if formatters.get(x).is_some() {
            fields.push(x);
        } else {
            others.insert(x);
        }
    }
    (fields, others)
}

/// The `fields` are the names of formatting functions to get from the `formatters`, these are applied to the `data`.
/// Set `header` to true to print a first row with field names as a header (independent of csv).
/// Set `csv` to true to get CSV output instead of fixed-format.

pub fn format_data<'a, DataT, FmtT, CtxT>(
    output: &mut dyn io::Write,
    fields: &[&'a str],
    formatters: &HashMap<String, FmtT>,
    header: bool,
    csv: bool,
    data: Vec<DataT>, ctx: CtxT)
where
    FmtT: Fn(&DataT, CtxT) -> String,
    CtxT: Copy
{
    let mut cols = Vec::<Vec<String>>::new();
    cols.resize(fields.len(), vec![]);

    // TODO: For performance this could cache the results of the hash table lookups in a local
    // array, it's wasteful to perform a lookup for each field for each iteration.
    data.iter().for_each(|x| {
        let mut i = 0;
        for kwd in fields {
            cols[i].push(formatters.get(*kwd).unwrap()(x, ctx));
            i += 1;
        }
    });

    let fixed_width = !csv;

    if fixed_width {
        // The column width is the max across all the entries in the column (including header,
        // if present)
        let mut widths = vec![];
        widths.resize(fields.len(), 0);

        if header {
            let mut i = 0;
            for kwd in fields {
                widths[i] = usize::max(widths[i], kwd.len());
                i += 1
            }
        }

        let mut row = 0;
        while row < cols[0].len() {
            let mut col = 0;
            while col < fields.len() {
                widths[col] = usize::max(widths[col], cols[col][row].len());
                col += 1;
            }
            row += 1;
        }

        // Header
        if header {
            let mut i = 0;
            for kwd in fields {
                let w = widths[i];
                output.write(format!("{:w$}  ", kwd).as_bytes()).unwrap();
                i += 1;
            }
            output.write(b"\n").unwrap();
        }

        // Body
        let mut row = 0;
        while row < cols[0].len() {
            let mut col = 0;
            while col < fields.len() {
                let w = widths[col];
                output.write(format!("{:w$}  ", cols[col][row]).as_bytes()).unwrap();
                col += 1;
            }
            output.write(b"\n").unwrap();
            row += 1;
        }
    } else {
        // FIXME: Some fields may need to be quoted here.  We should probably use the CSV writer
        // if we can.
        if header {
            let mut i = 0;
            for kwd in fields {
                output.write(format!("{}{}",
                                     if i > 0 { "," } else { "" },
                                     kwd).as_bytes())
                    .unwrap();
                i += 1;
            }
            output.write(b"\n").unwrap();
        }

        // Body
        let mut row = 0;
        while row < cols[0].len() {
            let mut col = 0;
            while col < fields.len() {
                output.write(format!("{}{}",
                                     if col > 0 { "," } else { "" },
                                     cols[col][row]).as_bytes())
                    .unwrap();
                col += 1;
            }
            output.write(b"\n").unwrap();
            row += 1;
        }
    }
}
