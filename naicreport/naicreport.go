// Run `naicreport help` for help

package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

type CommonArgs struct {
	DataPath string
	From string
}

type MlCpuhogOp struct {
	Common *CommonArgs
}

func main() {
	operation := parse_command_line()
	switch e := operation.(type) {
	case *MlCpuhogOp:
		ml_cpuhog(e)
	default:

	}
}

// General "free CSV" reader, returns array of maps from field names to field values
func read_free_csv(path string) ([]map[string]string, error) {
	input, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	rdr := csv.NewReader(input)
	rdr.FieldsPerRecord = -1		  // Rows arbitrarily wide, and possibly uneven
	rows := make([]map[string]string, 10) // array of maps from field name to field value, unparsed
	for {
		fields, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		m := make(map[string]string)
		for _, f := range(fields) {
			ix := strings.IndexByte(f, '=')
			if ix == -1 {
				// ouch? just drop the field
				continue
			}
			m[f[:ix]] = f[ix+1:]
		}
	}
	input.Close()
	return rows, nil
}

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

func ml_cpuhog(op *MlCpuhogOp) {
}

func parse_command_line() any {
	if len(os.Args) < 2 {
		toplevelUsage(1);
	}
	switch os.Args[1] {
	case "help":
		toplevelUsage(0)

	case "ml-cpuhog":
		opts := flag.NewFlagSet(os.Args[0] + " ml-cpuhog", flag.ExitOnError);
		data_path := opts.String("data-path", "", "Root directory of data store (required)")
		from := opts.String("from", "1d", "Start of log window")
		opts.Parse(os.Args[2:])
		if *data_path == "" {
			fmt.Fprintf(os.Stderr, "-data-path requires a value\nUsage of %s ml-cpuhog:\n", os.Args[0])
			opts.PrintDefaults()
			os.Exit(1)
		}
		return &MlCpuhogOp {
			Common: &CommonArgs {
				DataPath: *data_path,
				From: *from,
			},
		}

	default:
		toplevelUsage(1)
	}
	panic("Should not happen")
}

func toplevelUsage(code int) {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n")
	fmt.Fprintf(os.Stderr, "  %s <verb> <option> ...\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "where <verb> is one of\n")
	fmt.Fprintf(os.Stderr, "  help\n")
	fmt.Fprintf(os.Stderr, "    Print help\n")
	fmt.Fprintf(os.Stderr, "  ml-cpuhog\n")
	fmt.Fprintf(os.Stderr, "    Analyze the cpuhog logs and generate a report of new violations\n")
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "All verbs accept -h to print verb-specific help\n")
	os.Exit(code)
}
