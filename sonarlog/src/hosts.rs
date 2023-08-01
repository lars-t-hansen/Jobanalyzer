/// Matcher for host names.

pub struct HostFilter {
    matchers: Vec<(bool, Vec<String>)>
}

impl HostFilter {

    /// Create a new, empty filter.

    pub fn new() -> HostFilter {
        HostFilter { matchers: vec![] }
    }

    /// `patterns` is a vector of individual element patterns.  If the vector is longer than the
    /// number of elements in the string being matched then the match will fail.  Otherwise, if
    /// `exhaustive` is true and the vector differs in length from the number of elements, then the
    /// match will fail.  Otherwise, the patterns in the vector are applied elementwise to (a prefix
    /// of) the element vector of the string, and the match succeeds if all the element matchs
    /// succeed.
    ///
    /// Each element pattern can be (currently) a simple string or (eventually) a more complicated
    /// glob expression.

    pub fn add_pattern(&mut self, patterns: Vec<String>, exhaustive: bool) {
        self.matchers.push((exhaustive, patterns));
    }

    /// Convenience method: split the string into components and add a pattern with those components.

    pub fn insert(&mut self, fqdn: &str) {
        self.add_pattern(fqdn.split('.').map(|x| x.to_string()).collect::<Vec<String>>(), false);
    }

    /// Return true iff the filter has no patterns.

    pub fn is_empty(&self) -> bool {
        self.matchers.len() == 0
    }

    /// Match s against the patterns and return true if it matches at least one pattern.

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
