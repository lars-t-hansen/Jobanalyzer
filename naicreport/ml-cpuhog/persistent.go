// Data persistence for ml-cpuhog.
//
// The job information is represented on disk in free CSV form.  This means there's some annoying
// serialization and deserialization work, but the data are textual and structured at the same time,
// and this is better for debugging, resilience, and growth, at least for now.

package ml_cpuhog

import (
	"naicreport/storage"
	"path"
	"strconv"
	"time"
)

// Read the job state from disk and return a parsed and error-checked data structure.  Bogus records
// are silently dropped.
//
// If the file is not found then this returns ...  Otherwise it returns a genuine I/O error that
// should be reported.

func readCpuhogState(dataPath string) (map[jobKey]*cpuhogState, error) {
	stateFilename := path.Join(dataPath, "cpuhog-state.csv")
	stateCsv, err := storage.ReadFreeCSV(stateFilename)
	if err != nil {
		// TODO: Need to deal with file-open error vs io error vs parse error
		return nil, err
	}
	state := make(map[jobKey]*cpuhogState)
	for _, repr := range(stateCsv) {
		sId, success := repr["id"]
		host, found := repr["host"]
		success = success && found
		sStartedOnOrBefore, found := repr["startedOnOrBefore"]
		success = success && found
		sFirstViolation, found := repr["firstViolation"]
		success = success && found
		sLastSeen, found := repr["lastSeen"]
		success = success && found
		sIsReported, found := repr["isReported"]
		success = success && found
		id, err := strconv.ParseUint(sId, 10, 32)
		success = success && err == nil
		startedOnOrBefore, err := time.Parse(time.RFC3339, sStartedOnOrBefore)
		success = success && err == nil
		firstViolation, err := time.Parse(time.RFC3339, sFirstViolation)
		success = success && err == nil
		lastSeen, err := time.Parse(time.RFC3339, sLastSeen)
		success = success && err == nil
		isReported, err := strconv.ParseBool(sIsReported)
		success = success && err == nil
		if !success {
			// Bogus record
			continue
		}
		key := jobKey { jobid_t(id), host }
		state[key] = &cpuhogState {
			jobid_t(id),
			host,
			startedOnOrBefore,
			firstViolation,
			lastSeen,
			isReported,
		}
	}
	return state, nil
}

// TODO: It's possible this should sort the output by increasing ID.  This basically amounts to
// creating an array of job IDs, sorting that, and then walking it and looking up data by ID when
// writing.  This is nice because it means that files can be diffed.
//
// TODO: It's possible this should rename the existing state file as a .bak file.

func writeCpuhogState(dataPath string, data map[jobKey]*cpuhogState) error {
	
}
	
