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
//
// Rough outline of how it works:
//
//  - enumerate the cpuhog log files for the last n days
//
//  - read all these log files and consolidate duplicates into a "job log"
//
//  - read the state file into the "job state"
//
//  - for each job in the job log
//
//    - if the job is not in the job state
//      - add it to the job state
//      - add it to a list of jobs to report
//    - else
//      - mark the job as seen in the job state
//
//  - for each job in the job state
//    - if the job is not present in the job log and is not marked as seen within the last 48hrs
//      - remove it
//
//  - save the state
//
//  - for each job in the list to report
//    - generate output for it

package mlcpuhog

import (
	"os"
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

func MlCpuhog(progname string, args []string) error {
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

	hogState, err := readCpuhogState(hogOpts.DataPath)
	_, isPathErr := err.(*os.PathError)
	if isPathErr {
		hogState = make(map[jobKey]*cpuhogState)
	} else if err != nil {
		return err
	}

	logs, err := readLogFiles(&hogOpts)
	if err != nil {
		return err
	}

	// TODO: Now do integration and reporting
	logs = logs

	return writeCpuhogState(hogOpts.DataPath, hogState)
}

