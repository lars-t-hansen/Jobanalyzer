package main

import (
	"naicreport/storage"
)

// read the cpuhog report
// this has these fields at present:
//   tag=cpuhog
//   now=<timestamp>
//   jobm=<job+mark>
//   user=<username>
//   duration=..
//   host=<hostname>
//   cpu-peak=..
//   gpu-peak=..
//   rcpu-avg=..
//   rcpu-peak=..
//   rmem-avg=..
//   rmem-peak=..
//   start=..
//   end=..
//   cmd=..

// The report runs every 12h (at least) examining data from the previous 24h
//  
// What we want:
//  - the job is a cpu hog and should be reported
//  - we don't want to report jobs redundantly
//  - the state thus has a list of jobs reported recently
//  - a job is (probably) just a job number
//  - a job is purged from the state if it has not been seen for 48h
//  - the report is (for now) some textual output of the form:
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

// The state is probably just a GOB, for now, kept in the sonar root dir(?)
// The name of the state is ml-cpuhog-state.gob
//
// It may be that for now we want a flexible non-gob(?) format, eg csv being read into a map
// If it's going to be csv then there will be one line per process
//   job=,reported=,lastseen=
//
// There are some real problems with job#s because
//   - with slurm we have cross-host job numbers and must *not* use host
//   - on the ml nodes we must use (host,job) probably, but using command is dodgy because the
//     command "name" can change as processes come and go.  Plus, it's all python.

// So rough order of business:
//  - enumerate the cpuhog log files for the last n days (default is probably 1d but
//    somewhat likely we'll want to seed with a longer period than that)
//  - read all these log files and consolidate duplicates into per-job records with
//    start and end times and durations (tbd)
//  - read the state file
//  - for each job in cpuhog list
//    - if the job has not been reported
//      - add it to list of jobs to report
//      - mark as reported
//    - note that the job has been seen at this(?) time
//  - for each job in the cpuhog list
//    - if a job is not marked as seen within the last 48hrs
//      - remove it
//  - save the state
//  - for each job in the list to report
//    - generate output for it
//
// 

type MlCpuhogOp struct {
}

func MlCpuhog(common *CommonArgs, op *MlCpuhogOp) error {
	files, err := storage.EnumerateFiles(common.DataPath, common.From, "cpuhog.csv")
	if err != nil {
		return err
	}

	for _, file_path := range files {
		data, err := storage.ReadFreeCSV(file_path)
		if err != nil {
			continue
		}
		// consolidate into per-job records
	}

	state_path := path.join(common.DataPath, "cpuhog-state.csv");
	state, err := storage.ReadFreeCSV(state_path)
	if err != nil {
		// TODO: If the file is simply missing then this is not an error.
		return err
	}

	//	...;

	err = storage.WriteFreeCSV(state_path, new_state)
	if err != nil {
		return err
	}

	return nil
}

