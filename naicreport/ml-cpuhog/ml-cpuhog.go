// The cpuhog analysis runs every 12h (at least) examining data from the previous 24h, and will
// append information about CPU hogs to a log, with a fair amount of redundancy under normal
// circumstances.  The present component filters / resolves the redundancy and creates formatted
// reports about new violations.  For this it maintains state about what it's already reported.
//  
// Requirements:
//
//  - a job that appears in the log is a cpu hog and should be reported
//  - the report is (for now) some textual output of the form shown below
//  - we don't want to report jobs redundantly, so there will be state
//  - we don't want the state to grow without bound
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
//    that will just end up being emailed by cron, which is fine
//
// For now this code is specific to the ML nodes, hence the "ml" in all the names.
//
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

package ml_cpuhog

import (
	"path"
	"time"
)

// Options for this component

type MlCpuhogOp struct {
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
//
type jobKey struct {
	id jobid_t
	host string
}

// Information about CPU hogs stored in the persistent state.  Other data that are needed for
// generating the report can be picked up from the log data for the job ID.

type cpuhogState struct {
	id jobid_t
	host string
	startedOnOrBefore time.Time
	firstViolation time.Time
	lastSeen time.Time
	isReported bool
}

// The logState represents the view of a job across all the records read from the logs.  Here, too,
// (job#, host) identifies the job uniquely.  

type logState struct {
	id jobid_t
	host string
	user string
	cmd string					// ???
	duration time.Duration		// ???
	firstSeen time.Time			// timestamp of record in which job is first seen
	lastSeen time.Time			// ditto the record in which the job is last seen
	start time.Time				// the start field of the first record for the job
	end time.Time				// the end field of the last record for the job
	cpuPeak float64				// this and the following are all for the last 
	gpuPeak float64				//   record seen for the job, I think, as the
	rcpuAvg float64				//     log will have the correct data for that
	rcpuPeak float64
	rmemAvg float64
	rmemPeak float64
}

func MlCpuhog(op *MlCpuhogOp) error {
	cpuhog_state, err := readCpuhogState(op.DataPath)

	//	...;

	err = writeCpuhogState(state_path, new_state)
	if err != nil {
		return err
	}

	return nil
}

