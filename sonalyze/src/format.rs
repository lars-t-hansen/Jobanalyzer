// Generic formatting code for a set of data extracted from a data structure to be presented
// columnar or as csv, with or without a header.

use std::collections::{HashMap, HashSet};

/// Return a vector of the known fields in `spec` wrt the formatters, and a HashSet of any other
/// strings found in `spec`

pub fn parse_fields<'a, DataT, FmtT, CtxT>(spec: &'a str, formatters: &HashMap<String, FmtT>) -> (Vec<&'a str>, HashSet<&'a str>)
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

pub fn format_data<'a, DataT, FmtT, CtxT>(fields: &[&'a str], formatters: &HashMap<String, FmtT>, header: bool, csv: bool, data: Vec<DataT>, ctx: CtxT)
where
    FmtT: Fn(&DataT, CtxT) -> String,
    CtxT: Copy
{
    let mut cols = Vec::<Vec<String>>::new();
    cols.resize(fields.len(), vec![]);

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
                print!("{:w$}  ", kwd);
                i += 1;
            }
            println!("");
        }

        // Body
        let mut row = 0;
        while row < cols[0].len() {
            let mut col = 0;
            while col < fields.len() {
                let w = widths[col];
                print!("{:w$}  ", cols[col][row]);
                col += 1;
            }
            println!("");
            row += 1;
        }
    } else {
        // FIXME: Some fields may need to be quoted here.  We should probably use the CSV writer
        // if we can.
        if header {
            let mut i = 0;
            for kwd in fields {
                print!("{}{}", if i > 0 { "," } else { "" }, kwd);
                i += 1;
            }
            println!("");
        }

        // Body
        let mut row = 0;
        while row < cols[0].len() {
            let mut col = 0;
            while col < fields.len() {
                print!("{}{}", if col > 0 { "," } else { "" }, cols[col][row]);
                col += 1;
            }
            println!("");
            row += 1;
        }
    }
}
