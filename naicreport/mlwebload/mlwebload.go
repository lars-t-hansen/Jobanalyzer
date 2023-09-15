// Generate data for plotting the running load of the ML systems.  The data are taken from the live
// sonar logs, by means of sonalyze.

package mlwebload

import (
	"encoding/json"
	"errors"
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
	// Parse and sanitize options

	progOpts := util.NewStandardOptions(progname + " ml-webload")
	sonalyzePathPtr := progOpts.Container.String("sonalyze", "", "Path to sonalyze executable (required)")
	configPathPtr := progOpts.Container.String("config-file", "", "Path to system config file (required)")
	outputPathPtr := progOpts.Container.String("output-path", ".", "Path to output directory")
	tagPtr := progOpts.Container.String("tag", "", "Tag for output files")
	hourlyPtr := progOpts.Container.Bool("hourly", true, "Bucket data hourly")
	dailyPtr := progOpts.Container.Bool("daily", false, "Bucket data daily")
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
	outputPath, err := util.CleanPath(*outputPathPtr, "-output-path")
	if err != nil {
		return err
	}
		
	// Assemble sonalyze arguments and run it, collecting its output

	arguments := []string{
		"load",
		"--data-path", progOpts.DataPath,
		"--config-file", configPath,
		"--fmt=csvnamed," + sonalyzeFormat,
	};
	if progOpts.HaveFrom {
		arguments = append(arguments, "--from", progOpts.FromStr)
	}
	if progOpts.HaveTo {
		arguments = append(arguments, "--to", progOpts.ToStr)
	}
	// This isn't completely clean but it's good enough for not-insane users.
	// We can use flag.Visit() to do a better job.  This is true in general.
	if *dailyPtr {
		arguments = append(arguments, "--daily")
	} else if *hourlyPtr {
		arguments = append(arguments, "--hourly")
	} else {
		return errors.New("One of --daily or --hourly is required")
	}

	cmd := exec.Command(sonalyzePath, arguments...)
	var stdout strings.Builder
	var stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		return errors.Join(err, errors.New(stderr.String()))
	}

	// Interpret the output from sonalyze

	output, err := parseOutput(stdout.String())
	if err != nil {
		return err
	}

	// Convert selected fields to JSON

	return writePlots(outputPath, *tagPtr, output)
}

func writePlots(outputPath, tag string, output []*hostData) error {
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
		var basename string
		if tag == "" {
			basename = hd.hostname + ".json"
		} else {
			basename = hd.hostname + "-" + tag + ".json"
		}
		filename := path.Join(outputPath, basename)
		output_file, err := os.CreateTemp(path.Dir(filename), "naicreport-webload")
		if err != nil {
			return err
		}

		rcpuData := make([]perPoint, 0)
		rgpuData := make([]perPoint, 0)
		rmemData := make([]perPoint, 0)
		rgpumemData := make([]perPoint, 0)
		var x uint
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

const (
	sonalyzeFormat = "datetime,cpu,mem,gpu,gpumem,rcpu,rmem,rgpu,rgpumem,gpus,host"
)

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

// The output from sonalyze is sorted first by host, then by increasing time.  Thus it's fine to
// read record-by-record, bucket by host easily, and then assume that data are sorted within host.

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
