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

package mlbughunt

import (
	"fmt"
	"os"
	//	"sort"
	"time"

	"naicreport/jobstate"
	"naicreport/util"
)

const (
	bughuntFilename = "bughunt-state.csv"
)

type job struct {
	id uint32
	host string
	user string
	command string
	start time.Time
	end time.Time
	lastSeen time.Time
}

func MlBughunt(progname string, args []string) error {
	progOpts := util.NewStandardOptions(progname + "ml-bughunt")
	err := progOpts.Parse(args)
	if err != nil {
		return err
	}

	state, err := jobstate.ReadJobStateOrEmpty(progOpts.DataPath, bughuntFilename)
	if err != nil {
		return err
	}

	logs, err := readLogFiles(progOpts.DataPath, progOpts.From, progOpts.To)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	candidates := 0
	for _, job := range logs {
		if jobstate.EnsureJob(state, job.id, job.host, job.start, now, job.lastSeen) {
			candidates++
		}
	}
	if progOpts.Verbose {
		fmt.Fprintf(os.Stderr, "%d candidates\n", candidates)
	}

	purged := jobstate.Purge(state, progOpts.To)
	if progOpts.Verbose {
		fmt.Fprintf(os.Stderr, "%d purged\n", purged)
	}

	// FIXME: create report

	return nil
}

func readLogFiles(dataPath string, from, to time.Time) ([]job, error) {
	return nil, nil
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
