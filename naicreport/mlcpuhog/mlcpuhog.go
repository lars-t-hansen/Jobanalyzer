// The ml-nodes cpuhog analysis runs every 12h (at least - currently it runs every 2 hours, for
// testing purposes), examining data from the previous 24h, and will append information about CPU
// hogs to a daily log.  This generates a fair amount of redundancy under normal circumstances.
//
// The present component runs occasionally (tbd) and filters / resolves the redundancy and creates
// formatted reports about new violations.  For this it maintains state about what it's already seen
// and reported.
//
// For now this code is specific to the ML nodes, hence the "ml" in all the names.
//
// Requirements:
//
//  - a job that appears in the cpuhog log is a cpu hog and should be reported
//  - the report is (for now) some textual output of the form shown below
//  - we don't want to report jobs redundantly, so there will have to be persistent state
//  - we don't want the state to grow without bound
//
// Report format:
//
//     New CPU hog detected (uses a lot of CPU and no GPU) on host "XX":
//       User: username
//       Command: command name
//       Violation first detected: <date>  // this is the timestamp of the earliest record
//       Started on or before: <date>      // this is the start-time in the earliest record
//       Observed data:
//          CPU peak = n cores
//          CPU utilization avg/peak = n%, m%
//          Memory utilization avg/peak = n%, m%

package mlcpuhog

import (
	"cmp"
	"fmt"
	"os"
	"slices"
	"time"

	"naicreport/util"
)

// Options for this component

type cpuhogOptions struct {
	// The root directory of the data store, this must be an absolute and clean directory name.
	DataPath string

	// The earliest date that we're interested in, this should have a time component of zero.
	From time.Time

	// The earliest date that we're interested in, this should have a time component of zero.
	To time.Time
}

// For things we will see in this component, the job# will never be zero.

type jobid_t uint32

// On the ML nodes, (job#, host) identifies a job uniquely because job#s are not coordinated across
// hosts and no job is cross-host.
type jobKey struct {
	id   jobid_t
	host string
}

// Information about CPU hogs stored in the persistent state.  Other data that are needed for
// generating the report can be picked up from the log data for the job ID.

type cpuhogState struct {
	id                jobid_t
	host              string
	startedOnOrBefore time.Time
	firstViolation    time.Time
	lastSeen          time.Time
	isReported        bool
}

// The logState represents the view of a job across all the records read from the logs.  Here, too,
// (job#, host) identifies the job uniquely.

type logState struct {
	id        jobid_t       // synthesized job id
	host      string        // a single host name, since ml nodes
	user      string        // user's login name
	cmd       string        // ???
	duration  time.Duration // ???
	firstSeen time.Time     // timestamp of record in which job is first seen
	lastSeen  time.Time     // ditto the record in which the job is last seen
	start     time.Time     // the start field of the first record for the job
	end       time.Time     // the end field of the last record for the job
	cpuPeak   float64       // this and the following are the Max across all
	gpuPeak   float64       //   records seen for the job, this is necessary
	rcpuAvg   float64       //     as sonalyze will have a limited window in which
	rcpuPeak  float64       //       to gather statistics and its view will change
	rmemAvg   float64       //         over time
	rmemPeak  float64       //
}

func (s *logState) key() jobKey {
	return jobKey { id: s.id, host: s.host }
}

func MlCpuhog(progname string, args []string) error {
	// Figure out options to determine data directory and date range
	progOpts := util.NewStandardOptions(progname)
	err := progOpts.Parse(args)
	if err != nil {
		return err
	}
	hogOpts := cpuhogOptions {
		DataPath: *progOpts.DataPath,
		From: progOpts.From,
		To: progOpts.To,
	}

	// Read the persistent state, it may be absent
	hogState, err := readCpuhogState(hogOpts.DataPath)
	_, isPathErr := err.(*os.PathError)
	if isPathErr {
		hogState = make(map[jobKey]*cpuhogState)
	} else if err != nil {
		return err
	}

	// Read the relevant logs and integrate them into a job log
	logs, err := readLogFiles(&hogOpts)
	if err != nil {
		return err
	}

	// Scan all jobs in the log, add the job to the state if it is not there, otherwise mark it as
	// seen today.

	candidates := make([]jobKey, 0)
	now := time.Now().UTC()
	for k, job := range logs {
		v, found := hogState[k]
		if !found {
			hogState[k] = &cpuhogState {
				id: job.id,
				host: job.host,
				startedOnOrBefore: job.start, // TODO: min ?
				firstViolation: now,
				lastSeen: now,
				isReported: false,
			}
			candidates = append(candidates, k)
		} else {
			v.lastSeen = now
		}
	}

	// Purge jobs from the state that haven't been seen in 48 hrs

	twoDaysAgo := now.AddDate(0, 0, -2)
	dead := make([]jobKey, 0)
	for k, jobState := range hogState {
		if jobState.lastSeen.Before(twoDaysAgo) {
			dead = append(dead, k)
		}
	}
	for _, k := range dead {
		delete(hogState, k)
	}

	// Report jobs that remain in the state and are unreported
	//
	// We accumulate these in an array, sort them by ascending job# (there could be other criteria)
	// and then print them.

	type hogReport struct {
		key jobKey
		report string
	}
	reports := make([]*hogReport, 0)
	for k, jobState := range hogState {
		if !jobState.isReported {
			jobState.isReported = true
			job, _ := logs[k]
			report := fmt.Sprintf(
`New CPU hog detected (uses a lot of CPU and no GPU) on host "%s":
  Job#: %d
  User: %s
  Command: %s
  Started on or before: %s
  Violation first detected: %s
  Observed data:
    CPU peak = %d cores
    CPU utilization avg/peak = %d%%, %d%%
    Memory utilization avg/peak = %d%%, %d%%

`,
				jobState.host,
				jobState.id,
				job.user,
				job.cmd,
				jobState.startedOnOrBefore.Format("2006-01-02 15:04"),
				jobState.firstViolation.Format("2006-01-02 15:04"),
				uint32(job.cpuPeak / 100),
				uint32(job.rcpuAvg),
				uint32(job.rcpuPeak),
				uint32(job.rmemAvg),
				uint32(job.rmemPeak))
			reports = append(reports, &hogReport { key: k, report: report })
		}
	}

	slices.SortFunc(reports, func(a, b *hogReport) int {
		if a.key.host != b.key.host {
			return cmp.Compare(a.key.host, b.key.host)
		}
		return cmp.Compare(a.key.id, b.key.id)
	})

	for _, r := range reports {
		fmt.Print(r.report)
	}

	return writeCpuhogState(hogOpts.DataPath, hogState)
}

