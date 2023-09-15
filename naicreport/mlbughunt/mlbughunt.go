// The ml-nodes bughunt analysis runs every 12h (at least - currently it runs every 2 hours, for
// testing purposes), examining data from the previous 24h, and will append information about
// zombies, defunct processes and other dead weight to a daily log.  This generates a fair amount of
// redundancy under normal circumstances.
//
// The present component runs occasionally (tbd) and filters / resolves the redundancy and creates
// formatted reports about new problems.  For this it maintains state about what it's already seen
// and reported.
//
// Requirements:
//
//  - a job that appears in the bughunt log is a bug and should be reported
//  - the report is (for now) some textual output of the form shown below
//  - we don't want to report jobs redundantly, so there will have to be persistent state
//  - we don't want the state to grow without bound
//
// Report format:
//
//  (tbd)

// NOTE: This is substantially a clone of the mlcpuhog report.  It would be useful to be able to
// share data and code.  In particular the "persistent" code looks ripe for some kind of "jobstate"
// data structure.

package mlbughunt

func MlBughunt(progname string, args []string) error {
	return nil
}

// Log fields

// now = timestamp
// jobm
// user
// duration
// host
// start
// end
// cmd
// tag
