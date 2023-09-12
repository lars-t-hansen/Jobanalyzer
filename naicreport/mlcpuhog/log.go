// Ingest cpuhog logs and integrate them into an overview of jobs across time.
// How integration is performed is defined with the logState structure.

package mlcpuhog

import (
	"math"
	"path"
	"time"

	"naicreport/storage"
)

// Read the cpuhog reports for the ML systems and integrate them into a joint database of job information.

func readLogFiles(options *cpuhogOptions) (map[jobKey]*logState, error) {
	files, err := storage.EnumerateFiles(options.DataPath, options.From, options.To, "cpuhog.csv")
	if err != nil {
		return nil, err
	}

	jobs := make(map[jobKey]*logState)
	for _, file_path := range files {
		records, err := storage.ReadFreeCSV(path.Join(options.DataPath, file_path))
		if err != nil {
			continue
		}

		for _, r := range records {
			success := true

			tag := storage.GetString(r, "tag", &success)
			success = success && tag == "cpuhog"
			now := storage.GetDateTime(r, "now", &success)
			tmpid := storage.GetJobMark(r, "jobm", &success)
			id := jobid_t(tmpid)
			user := storage.GetString(r, "user", &success)
			host := storage.GetString(r, "host", &success)
			cmd := storage.GetString(r, "cmd", &success)
			cpuPeak := storage.GetFloat64(r, "cpu-peak", &success)
			gpuPeak := storage.GetFloat64(r, "gpu-peak", &success)
			rcpuAvg := storage.GetFloat64(r, "rcpu-avg", &success)
			rcpuPeak := storage.GetFloat64(r, "rcpu-peak", &success)
			rmemAvg := storage.GetFloat64(r, "rmem-avg", &success)
			rmemPeak := storage.GetFloat64(r, "rmem-peak", &success)
			start := storage.GetDateTime(r, "start", &success)
			end := storage.GetDateTime(r, "end", &success)

			if !success {
				continue
			}

			key := jobKey{id, host}
			if r, present := jobs[key]; present {
				// id, user, and host are fixed - host b/c this is the view of a job on the ml nodes
				// FIXME: cmd can change b/c of sonalyze's view on the job.
				r.firstSeen = minTime(r.firstSeen, now)
				r.lastSeen = maxTime(r.lastSeen, now)
				r.start = minTime(r.start, start)
				r.end = maxTime(r.end, end)
				// FIXME: duration can change
				r.cpuPeak = math.Max(r.cpuPeak, cpuPeak)
				r.gpuPeak = math.Max(r.gpuPeak, gpuPeak)
				r.rcpuAvg = math.Max(r.rcpuAvg, rcpuAvg)
				r.rcpuPeak = math.Max(r.rcpuPeak, rcpuPeak)
				r.rmemAvg = math.Max(r.rmemAvg, rmemAvg)
				r.rmemPeak = math.Max(r.rmemPeak, rmemPeak)
			} else {
				firstSeen := now
				lastSeen := now
				duration := time.Duration(0) // FIXME
				jobs[key] = &logState{
					id,
					host,
					user,
					cmd,
					duration,
					firstSeen,
					lastSeen,
					start,
					end,
					cpuPeak,
					gpuPeak,
					rcpuAvg,
					rcpuPeak,
					rmemAvg,
					rmemPeak,
				}
			}
		}
	}

	return jobs, nil
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
