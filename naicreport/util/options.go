package util

import (
	"errors"
	"flag"
	"os"
	"path"
	"regexp"
	"strconv"
	"time"
)

// A container for some common options and a FlagSet that can be extended with more options.

type StandardOptions struct {
	Container *flag.FlagSet
	DataPath *string			// After parsing: absolute, Cleaned path
	From time.Time				// After parsing: UTC timestamp
	To time.Time				// After parsing: UTC timestamp

	fromStr *string				// Unparsed 'from' value
	toStr *string				// Unparsed 'to' value
}

// The idea is that the program calls NewStandardOptions to get a structure with standard options
// added to the FlagSet, and with some helpers to parse the arguments.  The program can add more
// flags to opts.container before calling the parser (saving the the flag pointers elsewhere) so
// that the parsing of everything is properly integrated.

func NewStandardOptions(progname string) *StandardOptions {
	container := flag.NewFlagSet(progname+" ml-cpuhog", flag.ExitOnError)
	dataPath := container.String("data-path", "", "Root directory of data store (required)")
	fromStr := container.String("from", "1d", "Start of log window")
	toStr := container.String("to", "", "End of log window")
	return &StandardOptions {
		Container: container,
		DataPath: dataPath,
		From: time.Now(),
		To: time.Now(),

		fromStr: fromStr,
		toStr: toStr,
	}
}

func (s *StandardOptions) Parse(args []string) error {
	err := s.Container.Parse(args)
	if err != nil {
		return err
	}

	// Clean the DataPath and make it absolute.

	if *s.DataPath == "" {
		return errors.New("-data-path requires a value")
	}
	if path.IsAbs(*s.DataPath) {
		*s.DataPath = path.Clean(*s.DataPath)
	} else {
		wd, err := os.Getwd()
		if err != nil {
			return err
		}
		*s.DataPath = path.Join(wd, *s.DataPath)
	}

	// Figure out the date range.  From has a sane default so always parse; To has no default so
	// grab current day if nothing is specified.

	s.From, err = matchWhen(*s.fromStr)
	if err != nil {
		return err
	}

	if *s.toStr == "" {
		s.To = time.Now().UTC()
	} else {
		s.To, err = matchWhen(*s.toStr)
		if err != nil {
			return err
		}
	}

	// For To, we really want tomorrow's date because the date range is not inclusive on the right.

	s.To = s.To.AddDate(0, 0, 1)
	s.To = time.Date(s.To.Year(), s.To.Month(), s.To.Day(), 0, 0, 0, 0, time.UTC)

	return nil
}

// The format of `from` and `to` is one of:
//  YYYY-MM-DD
//  Nd (days ago)
//  Nw (weeks ago)

var dateRe = regexp.MustCompile(`^(\d\d\d\d)-(\d\d)-(\d\d)$`)
var daysRe = regexp.MustCompile(`^(\d+)d$`)
var weeksRe = regexp.MustCompile(`^(\d+)w$`)

func matchWhen(s string) (time.Time, error) {
	probe := dateRe.FindSubmatch([]byte(s))
	if probe != nil {
		yyyy, _ := strconv.ParseUint(string(probe[1]), 10, 32)
		mm, _ := strconv.ParseUint(string(probe[2]), 10, 32)
		dd, _ := strconv.ParseUint(string(probe[3]), 10, 32)
		return time.Date(int(yyyy), time.Month(mm), int(dd), 0, 0, 0, 0, time.UTC), nil
	}
	probe = daysRe.FindSubmatch([]byte(s))
	if probe != nil {
		days, _ := strconv.ParseUint(string(probe[1]), 10, 32)
		t := time.Now().UTC().AddDate(0, 0, -int(days))
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), nil
	}
	probe = weeksRe.FindSubmatch([]byte(s))
	if probe != nil {
		weeks, _ := strconv.ParseUint(string(probe[1]), 10, 32)
		t := time.Now().UTC().AddDate(0, 0, -int(weeks)*7)
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), nil
	}
	return time.Now(), errors.New("Bad time specification")
}

