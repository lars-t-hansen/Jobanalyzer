// Ingest cpuhog logs and integrate them into an overview of jobs across time.
// How integration is performed is defined with the logState structure.

package ml_cpuhog

import (
    "naicreport/storage"
	"strconv"
	"strings"
	"time"
)

// Read the cpuhog reports for the ML systems and integrate them into a joint database of job information.

func readLogFiles(options *MlCpuhogOp) (map[jobKey]*logState, error) {
	files, err := storage.EnumerateFiles(options.DataPath, options.From, options.To, "cpuhog.csv")
	if err != nil {
		return nil, err
	}

	jobs := make(map[jobKey]*logState)
	for _, file_path := range files {
		records, err := storage.ReadFreeCSV(file_path)
		if err != nil {
			continue
		}

		for _, r := range(records) {
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
			now, err := time.Parse(time.RFC3339, sNow)
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
			start, err := time.Parse(time.RFC3339, sStart)
			success = success && err == nil
			end, err := time.Parse(time.RFC3339, sEnd)
			success = success && err == nil
			if !success {
				continue
			}

			key := jobKey { id, host }
			if r, present := jobs[key]; present {
				r.lastSeen = now
				r.end = end
				r.cpuPeak = cpuPeak
				r.gpuPeak = gpuPeak
				r.rcpuAvg = rcpuAvg
				r.rcpuPeak = rcpuPeak
				r.rmemAvg = rmemAvg
				r.rmemPeak = rmemPeak
			} else {
				firstSeen := now
				lastSeen := now
				duration := time.Duration(0) // FIXME
				jobs[key] = &logState {
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

	
