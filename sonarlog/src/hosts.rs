/// Matcher for host names.
///
/// The matcher holds a number of patterns, added with `add_pattern`.  Each `pattern` is a vector of
/// element patterns.
///
/// During matching, a string is turned into an element vector (elements separated by `.`), and the
/// patterns are matched one-by-one against the element vector.  If a pattern is longer than the
/// elements vector then the match will fail.  Otherwise, if `exhaustive` is true and the pattern
/// differs in length from the element vector, then the match will fail.  Otherwise, the pattern
/// elements are applied elementwise to (a prefix of) the element vector, and the match succeeds if
/// all the element matches succeed.
///
/// Grammar for patterns:
///
///  pattern ::= element+
///  element ::= eltpat ("," eltpat)*
///  eltpat ::= primitive+ star?
///  primitive ::= literal | range
///  literal ::= <character not containing '[' or '*' or '.' or ','> +
///  range ::= '[' range-elt ("," range-elt)* ']'
///  range-elt ::= number | number "-" number
///  star ::= '*'
///
/// The meaning of a range is that it is expanded into the set of numbers it contains; by inserting
/// these numbers into the eltpat we get a number of new eltpats, which are subject to further
/// expansion.  This is guaranteed to terminate since the expansion cannot yield further expandable
/// primitives.
///
/// Thus after expansion a pattern is a number of literal strings optionally with a * at the end,
/// denoting an open suffix.
use crate::pattern;

use anyhow::Result;

pub struct HostFilter {
    // The outer bool is `exhaustive`, the inner bool is `prefix`
    matchers: Vec<(bool, Vec<(bool, String)>)>,
}

impl HostFilter {
    /// Create a new, empty filter.

    pub fn new() -> HostFilter {
        HostFilter { matchers: vec![] }
    }

    /// Add a new pattern.

    pub fn add_pattern(&mut self, patterns: Vec<String>, exhaustive: bool) -> Result<()> {
        // Each element of `patterns` can be expanded into a set of strings and we basically need to
        // push a new pattern for each of these.  This is not the most efficient way to perform
        // matching but probably good enough, and in practice only the first element of `patterns`
        // will have multiple expansions.

        for patterns in expand_patterns(&patterns)? {
            self.matchers.push((exhaustive, patterns));
        }
        Ok(())
    }

    /// Convenience method: split the pattern string into element patterns and add a pattern with
    /// those element patterns.

    pub fn insert(&mut self, pattern: &str) -> Result<()> {
        self.add_pattern(
            pattern
                .split('.')
                .map(|x| x.to_string())
                .collect::<Vec<String>>(),
            false,
        )
    }

    /// Return true iff the filter has no patterns.

    pub fn is_empty(&self) -> bool {
        self.matchers.len() == 0
    }

    /// Match s against the patterns and return true iff it matches at least one pattern.

    pub fn contains(&self, s: &str) -> bool {
        let components = s.split('.').collect::<Vec<&str>>();
        'try_matcher: for (exhaustive, pattern) in &self.matchers {
            if pattern.len() > components.len() {
                continue 'try_matcher;
            }
            if *exhaustive && pattern.len() != components.len() {
                continue 'try_matcher;
            }
            for i in 0..pattern.len() {
                let (prefix, ref pattern) = pattern[i];
                if prefix {
                    if !components[i].starts_with(pattern) {
                        continue 'try_matcher;
                    }
                } else {
                    if components[i] != pattern {
                        continue 'try_matcher;
                    }
                }
            }
            return true;
        }
        return false;
    }
}

fn expand_patterns(xs: &[String]) -> Result<Vec<Vec<(bool, String)>>> {
    if xs.len() == 0 {
        Ok(vec![vec![]])
    } else {
        let rest = expand_patterns(&xs[1..])?;
        let expanded = pattern::expand_element(&xs[0])?;
        let mut result = vec![];
        for e in expanded {
            for r in &rest {
                let is_prefix = e.ends_with('*');
                let text = if is_prefix {
                    e[..e.len() - 1].to_string()
                } else {
                    e.to_string()
                };
                let mut m = vec![(is_prefix, text)];
                m.extend_from_slice(&r);
                result.push(m);
            }
        }
        Ok(result)
    }
}

#[test]
fn test_hostfilter1() {
    let mut hf = HostFilter::new();
    hf.add_pattern(vec!["ml8".to_string()], false).unwrap();
    hf.add_pattern(
        vec![
            "ml4".to_string(),
            "hpc".to_string(),
            "uio".to_string(),
            "no".to_string(),
        ],
        true,
    )
    .unwrap();
    hf.add_pattern(vec!["ml3".to_string(), "hpc".to_string()], false)
        .unwrap();

    // Single-element prefix match against this
    assert!(hf.contains("ml8.hpc.uio.no"));

    // Multi-element prefix match against this
    assert!(hf.contains("ml3.hpc.uio.no"));

    // Exhaustive match against this
    assert!(hf.contains("ml4.hpc.uio.no"));
    assert!(!hf.contains("ml4.hpc.uio.no.yes"));
}

#[test]
fn test_hostfilter2() {
    let mut hf = HostFilter::new();
    hf.add_pattern(vec!["ml[1-3]*".to_string()], false).unwrap();
    assert!(hf.contains("ml1"));
    assert!(hf.contains("ml1x"));
    assert!(hf.contains("ml1.uio"));
}

#[test]
fn test_expansion() {
    assert!(
        expand_patterns(&vec!["hi[1-2]*".to_string(), "ho[3-4]".to_string()])
            .unwrap()
            .eq(&vec![
                vec![(true, "hi1".to_string()), (false, "ho3".to_string())],
                vec![(true, "hi1".to_string()), (false, "ho4".to_string())],
                vec![(true, "hi2".to_string()), (false, "ho3".to_string())],
                vec![(true, "hi2".to_string()), (false, "ho4".to_string())]
            ])
    )
}
