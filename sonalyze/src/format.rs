// Generic formatting code for a set of data extracted from a data structure to be presented
// columnar or as csv, with or without a header.

use std::collections::{HashMap, HashSet};
use std::io;

/// Return a vector of the known fields in `spec` wrt the formatters, and a HashSet of any other
/// strings found in `spec`

pub fn parse_fields<'a, DataT, FmtT, CtxT>(
    spec: &'a str,
    formatters: &HashMap<String, FmtT>,
    aliases: &'a HashMap<String, Vec<String>>,
) -> (Vec<&'a str>, HashSet<&'a str>)
where
    FmtT: Fn(&DataT, CtxT) -> String,
    CtxT: Copy,
{
    let mut others = HashSet::new();
    let mut fields = vec![];
    for x in spec.split(',') {
        if formatters.get(x).is_some() {
            fields.push(x);
        } else if let Some(aliases) = aliases.get(x) {
            for alias in aliases {
                if formatters.get(alias).is_some() {
                    fields.push(alias.as_ref());
                } else {
                    others.insert(alias.as_ref());
                }
            }
        } else {
            others.insert(x);
        }
    }
    (fields, others)
}

pub struct FormatOptions {
    pub tag: Option<String>,
    pub header: bool,
    pub csv: bool,
    pub named: bool,
}

pub fn standard_options(others: &HashSet<&str>) -> FormatOptions {
    let csvnamed = others.get("csvnamed").is_some();
    let csv = others.get("csv").is_some() || csvnamed;
    let header =
        (!csv && !others.get("noheader").is_some()) || (csv && others.get("header").is_some());
    let mut tag : Option<String> = None;
    for x in others {
        if x.starts_with("tag:") {
            tag = Some(x[4..].to_string());
            break;
        }
    }
    FormatOptions {
        csv,
        header,
        tag,
        named: csvnamed
    }
}

/// The `fields` are the names of formatting functions to get from the `formatters`, these are
/// applied to the `data`.  Set `opts.header` to true to print a first row with field names as a
/// header (independent of csv).  Set `opts.csv` to true to get CSV output instead of fixed-format.
/// Set `opts.tag` to Some(s) to print a tag=s field in the output.

pub fn format_data<'a, DataT, FmtT, CtxT>(
    output: &mut dyn io::Write,
    fields: &[&'a str],
    formatters: &HashMap<String, FmtT>,
    opts: &FormatOptions,
    data: Vec<DataT>,
    ctx: CtxT,
) where
    FmtT: Fn(&DataT, CtxT) -> String,
    CtxT: Copy,
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

    let fixed_width = !opts.csv;

    if fixed_width {
        // The column width is the max across all the entries in the column (including header,
        // if present).  If there's a tag, it is printed in the last column.
        let mut widths = vec![];
        widths.resize(fields.len() + if opts.tag.is_some() { 1 } else { 0 }, 0);

        if opts.header {
            let mut i = 0;
            for kwd in fields {
                widths[i] = usize::max(widths[i], kwd.len());
                i += 1;
            }
            if opts.tag.is_some() {
                widths[i] = usize::max(widths[i], "tag".len());
            }
        }

        let mut row = 0;
        while row < cols[0].len() {
            let mut col = 0;
            while col < fields.len() {
                widths[col] = usize::max(widths[col], cols[col][row].len());
                col += 1;
            }
            if let Some(ref tag) = opts.tag {
                widths[col] = usize::max(widths[col], tag.len());
            }
            row += 1;
        }

        // Header
        if opts.header {
            let mut i = 0;
            for kwd in fields {
                let w = widths[i];
                output.write(format!("{:w$}  ", kwd).as_bytes()).unwrap();
                i += 1;
            }
            if opts.tag.is_some() {
                let w = widths[i];
                output.write(format!("{:w$}  ", "tag").as_bytes()).unwrap();
            }
            output.write(b"\n").unwrap();
        }

        // Body
        let mut row = 0;
        while row < cols[0].len() {
            let mut col = 0;
            while col < fields.len() {
                let w = widths[col];
                output
                    .write(format!("{:w$}  ", cols[col][row]).as_bytes())
                    .unwrap();
                col += 1;
            }
            if let Some(ref tag) = opts.tag {
                let w = widths[col];
                output
                    .write(format!("{:w$}  ", tag).as_bytes())
                    .unwrap();
            }
            output.write(b"\n").unwrap();
            row += 1;
        }
    } else {
        // FIXME: Some fields may need to be quoted here.  We should probably use the CSV writer
        // if we can.
        if opts.header {
            let mut i = 0;
            for kwd in fields {
                output
                    .write(format!("{}{}", if i > 0 { "," } else { "" }, kwd).as_bytes())
                    .unwrap();
                i += 1;
            }
            if opts.tag.is_some() {
                output
                    .write(format!("{}{}", if i > 0 { "," } else { "" }, "tag").as_bytes())
                    .unwrap();
            }
            output.write(b"\n").unwrap();
        }

        // Body
        let mut row = 0;
        while row < cols[0].len() {
            let mut col = 0;
            while col < fields.len() {
                let name = if opts.named { format!("{}=", fields[col]) } else { "".to_string() };
		let sep = if col > 0 { "," } else { "" };
		let val = &cols[col][row];
                output
                    .write(
                        format!("{sep}{name}{val}").as_bytes(),
                    )
                    .unwrap();
                col += 1;
            }
            if let Some(ref tag) = opts.tag {
                let name = if opts.named { "tag=" } else { "" };
		let sep = if col > 0 { "," } else { "" };
                output
                    .write(format!("{sep}{name}{tag}").as_bytes())
                    .unwrap();
            }
            output.write(b"\n").unwrap();
            row += 1;
        }
    }
}
