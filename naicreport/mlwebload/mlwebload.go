// Generate data for plotting the running load of the ML systems.  The data are taken from the live
// sonar logs, by means of sonalyze.

// Rough design:
//
// - run sonalyze for some time range and capture the desired output
// - parse the output into an internal form
// - generate plottable data
// - emit plottable data to a file
// - somehow signal that the file has been updated (eg by git-commit)

package mlwebload

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"naicreport/storage"
	"naicreport/util"
)

func MlWebload(progname string, args []string) error {
	progOpts := util.NewStandardOptions(progname)
	sonalyzePathPtr := progOpts.Container.String("sonalyze", "", "Path to sonalyze executable (required)")
	configPathPtr := progOpts.Container.String("config-file", "", "Path to system config file (required)")
	err := progOpts.Parse(args)
	if err != nil {
		return err
	}
	sonalyzePath, err := util.CleanPath(*sonalyzePathPtr, "-sonalyze")
	if err != nil {
		return err
	}
	configPath, err := util.CleanPath(*configPathPtr, "-config-file")
	if err != nil {
		return err
	}

	// Assemble arguments and run sonalyze, collecting output

	// TODO: --host filter, pass it on
	arguments := []string{
		"load",
		"--data-path", progOpts.DataPath,
		"--config-file", configPath,
		"--hourly",
		"--fmt=csvnamed,datetime,cpu,mem,gpu,gpumem,rcpu,rmem,rgpu,rgpumem,gpus,host",
	};
	if progOpts.HaveFrom {
		arguments = append(arguments, "--from", progOpts.FromStr)
	}
	if progOpts.HaveTo {
		arguments = append(arguments, "--to", progOpts.ToStr)
	}
		
	cmd := exec.Command(sonalyzePath, arguments...)
	var stdout strings.Builder
	var stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		// FIXME: Really return a combined error of stderr and the original error
		fmt.Fprintf(os.Stderr, "ERROR:\n%s", stderr.String())
		return err
	}

	output, err := parseOutput(stdout.String())
	if err != nil {
		return err
	}

	// Now we have a by-hostname list where data are sorted by increasing time within each host.
	// We just need to present it in some sensible way.

	/*
	for _, hd := range output {
		fmt.Printf("%s\n", hd.hostname)
		for _, d := range hd.data {
			fmt.Printf("  %v %v %v %v %v\n", d.cpu, d.mem, d.gpu, d.gpumem, d.gpus)
		}
	}
	*/

	return writePlots(output)
}

func writePlots(output []*hostData) error {
	type perPoint struct {
		X uint     `json:"x"`
		Y float64  `json:"y"`
	}

	type perHost struct {
		Hostname string      `json:"hostname"`
		Start string         `json:"start"`
		End string           `json:"end"`
		Rcpu []perPoint      `json:"rcpu"`
		Rgpu []perPoint      `json:"rgpu"`
		Rmem []perPoint      `json:"rmem"`
		Rgpumem []perPoint   `json:"rgpumem"`
	}

	timeFormat := "2006-01-02 15:04"
	for _, hd := range output {
		// TODO: output-dir would be helpful?
		// TODO: This is some generic call-with-tempfile function, see also storage.go
		// for the same pattern
		filename := path.Join(".", hd.hostname + ".json")
		output_file, err := os.CreateTemp(path.Dir(filename), "naicreport-webload")
		if err != nil {
			return err
		}

		rcpuData := make([]perPoint, 0)
		rgpuData := make([]perPoint, 0)
		rmemData := make([]perPoint, 0)
		rgpumemData := make([]perPoint, 0)
		x := uint(0)
		for _, d := range hd.data {
			rcpuData = append(rcpuData, perPoint { x, d.rcpu })
			rgpuData = append(rgpuData, perPoint { x, d.rgpu })
			rmemData = append(rmemData, perPoint { x, d.rmem })
			rgpumemData = append(rgpumemData, perPoint { x, d.rgpumem })
			x++
		}
		t := hd.data[0].datetime
		bytes, err := json.Marshal(perHost {
			Hostname: hd.hostname,
			Start: t.Format(timeFormat),
			End: t.Add(time.Hour).Format(timeFormat),
			Rcpu: rcpuData,
			Rgpu: rgpuData,
			Rmem: rmemData,
			Rgpumem: rgpumemData,
		})
		if err != nil {
			return err
		}
		output_file.Write(bytes)

		oldname := output_file.Name()
		output_file.Close()
		os.Rename(oldname, filename)
	}

	return nil
}

type datum struct {
	datetime time.Time
	cpu float64
	mem float64
	gpu float64
	gpumem float64
	gpus []uint32				// nil for "unknown"
	rcpu float64
	rmem float64
	rgpu float64
	rgpumem float64
	hostname string				// redundant but maybe useful
}

type hostData struct {
	hostname string
	data []*datum
}

// The output is sorted by increasing time, with a run of records for each host, and host names
// are sorted lexicographically (though this may change a little).  Thus it's fine to read
// record-by-record, bucket by host easily, and then assume that data are sorted within host.
	
func parseOutput(output string) ([]*hostData, error) {
	rows, err := storage.ParseFreeCSV(strings.NewReader(output))
	if err != nil {
		return nil, err
	}

	allData := make([]*hostData, 0)

	var curData []*datum
	curHost := ""
	for _, row := range rows {
		success := true
		newHost := storage.GetString(row, "host", &success)
		if !success {
			continue
		}
		if newHost != curHost {
			if curData != nil {
				allData = append(allData, &hostData { hostname: curHost, data: curData })
			}
			curData = make([]*datum, 0)
			curHost = newHost
		}
		newDatum := &datum {
			datetime: storage.GetDateTime(row, "datetime", &success),
			cpu: storage.GetFloat64(row, "cpu", &success),
			mem: storage.GetFloat64(row, "mem", &success),
			gpu: storage.GetFloat64(row, "gpu", &success),
			gpumem: storage.GetFloat64(row, "gpumem", &success),
			gpus: nil,
			rcpu: storage.GetFloat64(row, "rcpu", &success),
			rmem: storage.GetFloat64(row, "rmem", &success),
			rgpu: storage.GetFloat64(row, "rgpu", &success),
			rgpumem: storage.GetFloat64(row, "rgpumem", &success),
			hostname: newHost,
		}
		gpuRepr := storage.GetString(row, "gpus", &success)
		var gpuData []uint32		// Unknown set
		if gpuRepr != "unknown" {
			gpuData = make([]uint32, 0) // Empty set
			if gpuRepr != "none" {
				for _, t := range strings.Split(gpuRepr, ",") {
					n, err := strconv.ParseUint(t, 10, 32)
					if err == nil {
						gpuData = append(gpuData, uint32(n))
					}
				}
			}
		}
		newDatum.gpus = gpuData
		if !success {
			continue
		}
		curData = append(curData, newDatum)
	}
	if curData != nil {
		allData = append(allData, &hostData { hostname: curHost, data: curData })
	}

	return allData, nil
}
