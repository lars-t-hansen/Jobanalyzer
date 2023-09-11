package mlcpuhog

import (
	"io"
	"os"
	"path"
	"testing"
	"time"
)

func TestWriteState(t *testing.T) {
	s := make(map[jobKey]*cpuhogState)
	s1 := &cpuhogState {
		id: 10,
		host: "hello",
		startedOnOrBefore: time.Date(2023, 6, 14, 16, 0, 0, 0, time.UTC),
		firstViolation: time.Date(2023, 6, 15, 10, 20, 30, 0, time.UTC),
		lastSeen: time.Date(2023, 9, 11, 15, 37, 0, 0, time.UTC),
		isReported: false,
	}
	s[jobKey { id: s1.id, host: s1.host }] = s1

	td_name, err := os.MkdirTemp(os.TempDir(), "naicreport")
	if err != nil {
		t.Fatalf("MkdirTemp failed %q", err)
	}
	err = writeCpuhogState(td_name, s)
	if err != nil {
		t.Fatalf("Could not write: %q", err)
	}
	
	// First test: read raw text and make sure it looks OK

	f, err := os.Open(path.Join(td_name, "cpuhog-state.csv"))
	if err != nil {
		t.Fatalf("Open failed %q", err)
	}
	all, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll failed %q", err)
	}
	expect := "id=10,host=hello,startedOnOrBefore=2023-06-14T16:00:00Z,firstViolation=2023-06-15T10:20:30Z,lastSeen=2023-09-11T15:37:00Z,isReported=false\n"
	if string(all) != expect {
		t.Fatalf("File contents wrong %q", all)
	}
	f.Close()

	// Second test: read it as state and make sure the contents look good

	newState, err := readCpuhogState(td_name)
	if err != nil {
		t.Fatalf("readCpuhogState failed %q", err)
	}
	if len(newState) != 1 {
		t.Fatalf("Bad contents")
	}
	for k, v := range newState {
		if k.id != s1.id || k.host != s1.host {
			t.Fatalf("Bad key %q", k)
		}
		if v.id != s1.id || v.host != s1.host || !v.startedOnOrBefore.Equal(s1.startedOnOrBefore) ||
			!v.firstViolation.Equal(s1.firstViolation) || !v.lastSeen.Equal(s1.lastSeen) ||
			v.isReported != s1.isReported {
			t.Fatalf("Bad contents")
		}
	}
}
