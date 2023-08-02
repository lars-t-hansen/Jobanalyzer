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

pub struct HostFilter {
    matchers: Vec<(bool, Vec<String>)>
}

impl HostFilter {

    /// Create a new, empty filter.

    pub fn new() -> HostFilter {
        HostFilter { matchers: vec![] }
    }

    /// Add a new pattern.

    pub fn add_pattern(&mut self, patterns: Vec<String>, exhaustive: bool) {
        self.matchers.push((exhaustive, patterns));
    }

    /// Convenience method: split the pattern string into element patterns and add a pattern with
    /// those element patterns.

    pub fn insert(&mut self, pattern: &str) {
        self.add_pattern(pattern.split('.').map(|x| x.to_string()).collect::<Vec<String>>(), false);
    }

    /// Return true iff the filter has no patterns.

    pub fn is_empty(&self) -> bool {
        self.matchers.len() == 0
    }

    /// Match s against the patterns and return true iff it matches at least one pattern.

    pub fn contains(&self, s: &str) -> bool {
        let components = s.split('.').collect::<Vec<&str>>();
        'try_matcher:
        for (exhaustive, pattern) in &self.matchers {
            if pattern.len() > components.len() {
                continue 'try_matcher;
            }
            if *exhaustive && pattern.len() != components.len() {
                continue 'try_matcher;
            }
            for i in 0..pattern.len() {
                if !self.match_simple(&pattern[i], components[i]) {
                    continue 'try_matcher;
                }
            }
            return true
        }
        return false
    }

    fn match_simple(&self, pattern: &str, component: &str) -> bool {
        pattern == component
    }
}

#[test]
fn test_hostfilter() {
    let mut hf = HostFilter::new();
    hf.add_pattern(vec!["ml8".to_string()], false);
    hf.add_pattern(vec!["ml4".to_string(),"hpc".to_string(),"uio".to_string(), "no".to_string()], true);
    hf.add_pattern(vec!["ml3".to_string(), "hpc".to_string()], false);

    // Single-element prefix match against this
    assert!(hf.contains("ml8.hpc.uio.no"));

    // Multi-element prefix match against this
    assert!(hf.contains("ml3.hpc.uio.no"));

    // Exhaustive match against this
    assert!(hf.contains("ml4.hpc.uio.no"));
    assert!(!hf.contains("ml4.hpc.uio.no.yes"));
}
