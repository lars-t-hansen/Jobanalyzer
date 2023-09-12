// Ingest cpuhog logs and integrate them into an overview of jobs across time.
// How integration is performed is defined with the logState structure.

package mlcpuhog

import (
	"math"
	"path"
	"strconv"
	"strings"
	"time"

	"naicreport/storage"
)

// Read the cpuhog reports for the ML systems and integrate them into a joint database of job information.

func readLogFiles(options *MlCpuhogOp) (map[jobKey]*logState, error) {
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
			tag, success := r["tag"]
			success = success && tag == "cpuhog"
			sNow, found := r["now"]
			success = success && found
			sJobm, found := r["jobm"]
			success = success && found
			user, found := r["user"]
			success = success && found
			host, found := r["host"]
			success = success && found
			sCpuPeak, found := r["cpu-peak"]
			success = success && found
			sGpuPeak, found := r["gpu-peak"]
			success = success && found
			sRcpuAvg, found := r["rcpu-avg"]
			success = success && found
			sRcpuPeak, found := r["rcpu-peak"]
			success = success && found
			sRmemAvg, found := r["rmem-avg"]
			success = success && found
			sRmemPeak, found := r["rmem-peak"]
			success = success && found
			sStart, found := r["start"]
			success = success && found
			sEnd, found := r["end"]
			success = success && found
			cmd, found := r["cmd"]
			success = success && found
			id, ok := parse_jobm(sJobm)
			success = success && ok
			now, err := time.Parse("2006-01-02 15:04", sNow)
			success = success && err == nil
			cpuPeak, err := strconv.ParseFloat(sCpuPeak, 64)
			success = success && err == nil
			gpuPeak, err := strconv.ParseFloat(sGpuPeak, 64)
			success = success && err == nil
			rcpuAvg, err := strconv.ParseFloat(sRcpuAvg, 64)
			success = success && err == nil
			rcpuPeak, err := strconv.ParseFloat(sRcpuPeak, 64)
			success = success && err == nil
			rmemAvg, err := strconv.ParseFloat(sRmemAvg, 64)
			success = success && err == nil
			rmemPeak, err := strconv.ParseFloat(sRmemPeak, 64)
			success = success && err == nil
			start, err := time.Parse("2006-01-02 15:04", sStart)
			success = success && err == nil
			end, err := time.Parse("2006-01-02 15:04", sEnd)
			success = success && err == nil

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

// Jobm is job# optionally suffixed by '<', '>', or '!'.  Here we return the job# and true if we
// were able to parse it.

func parse_jobm(s string) (jobid_t, bool) {
	id, err := strconv.ParseUint(strings.TrimRight(s, "<>!"), 10, 32)
	return jobid_t(id), err == nil
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
