// Ingest cpuhog logs and integrate them into an overview of jobs across time.
// How integration is performed is defined with the logState structure.

package mlcpuhog

import (
	"math"
	"path"
	"time"

	"naicreport/jobstate"
	"naicreport/storage"
	"naicreport/util"
)

// Read the cpuhog reports for the ML systems and integrate them into a joint database of job information.

func readLogFiles(options *cpuhogOptions) (map[jobstate.JobKey]*logState, error) {
	files, err := storage.EnumerateFiles(options.DataPath, options.From, options.To, "cpuhog.csv")
	if err != nil {
		return nil, err
	}

	jobs := make(map[jobstate.JobKey]*logState)
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

			key := jobstate.JobKey{uint32(id), host}
			if r, present := jobs[key]; present {
				// id, user, and host are fixed - host b/c this is the view of a job on the ml nodes
				// FIXME: cmd can change b/c of sonalyze's view on the job.
				r.firstSeen = util.MinTime(r.firstSeen, now)
				r.lastSeen = util.MaxTime(r.lastSeen, now)
				r.start = util.MinTime(r.start, start)
				r.end = util.MaxTime(r.end, end)
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
