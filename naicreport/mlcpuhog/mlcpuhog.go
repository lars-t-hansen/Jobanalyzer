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
//       Job#: n
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
	"fmt"
	"os"
	"time"

	"naicreport/jobstate"
	"naicreport/util"
)

const (
	cpuhogFilename = "cpuhog-state.csv"
)

// Options for this component

type cpuhogOptions struct {
	DataPath string
	From time.Time
	To time.Time
}

// For things we will see in this component, the job# will never be zero.

type jobid_t uint32

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
	// Figure out options to determine data directory and date range.

	progOpts := util.NewStandardOptions(progname + "ml-cpuhog")
	err := progOpts.Parse(args)
	if err != nil {
		return err
	}
	hogOpts := cpuhogOptions {
		DataPath: progOpts.DataPath,
		From: progOpts.From,
		To: progOpts.To,
	}

	// Read the persistent state, it may be absent.

	hogState, err := jobstate.ReadJobStateOrEmpty(hogOpts.DataPath, cpuhogFilename)
	if err != nil {
		return err
	}

	// Read the relevant logs and integrate them into a job log.

	logs, err := readLogFiles(&hogOpts)
	if err != nil {
		return err
	}

	// Current time, used for all time stamps below.
	//
	// TODO: should this be shared with similar uses in eg the log and options processing code?

	now := time.Now().UTC()

	// Scan all jobs in the log, add the job to the state if it is not there, otherwise mark it as
	// seen today.

	candidates := 0
	for _, job := range logs {
		if jobstate.EnsureJob(hogState, uint32(job.id), job.host, job.start, now, job.lastSeen) {
			candidates++
		}
	}
	if progOpts.Verbose {
		fmt.Fprintf(os.Stderr, "%d candidates\n", candidates)
	}

	purged := jobstate.Purge(hogState, progOpts.To)
	if progOpts.Verbose {
		fmt.Fprintf(os.Stderr, "%d purged\n", purged)
	}

	// Generate reports for jobs that remain in the state and are unreported.

	reports := make([]*util.JobReport, 0)
	for k, jobState := range hogState {
		if !jobState.IsReported {
			jobState.IsReported = true
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
				jobState.Host,
				jobState.Id,
				job.user,
				job.cmd,
				jobState.StartedOnOrBefore.Format("2006-01-02 15:04"),
				jobState.FirstViolation.Format("2006-01-02 15:04"),
				uint32(job.cpuPeak / 100),
				uint32(job.rcpuAvg),
				uint32(job.rcpuPeak),
				uint32(job.rmemAvg),
				uint32(job.rmemPeak))
			reports = append(reports, &util.JobReport { Id: k.Id, Host: k.Host, Report: report })
		}
	}

	// Sort reports by ascending job key with host name as the major key and job ID as the minor key
	// (there could be other criteria) and print them.

	util.SortReports(reports)
	for _, r := range reports {
		fmt.Print(r.Report)
	}

	// Save the persistent state.

	return jobstate.WriteJobState(hogOpts.DataPath, cpuhogFilename, hogState)
}

