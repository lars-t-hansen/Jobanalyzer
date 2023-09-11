package mlcpuhog

import (
	"testing"
	"time"
)

func TestReadLogFiles(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd failed: %q", err)
	}

	// The time span here should correspond with what's in the directory
	// Initially we want just the one day and very carefully curated data
	op := MlCpuhogOp {
		DataPath: path.Join(wd, "../../sonar_test_data0"),
		From: time.Date(...),
		To: time.Date(...),
	}
	jobLog, err := readLogFiles(&op)
	if err != nil {
		t.Fatalf("Could not read: %q", err)
	}

	// This should test that we match the one record exactly

	// FIXME

	// Then redo the operation but now pick up two files, one has a newer record for the job.
	// We want to see some of the old data but also some of the new data.

	// FIXME
}
