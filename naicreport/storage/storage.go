// Storage management for naicreport.  For now, the file format is always "free CSV" form, that is,
// files have CSV syntax but each row can have a different number of columns and each column value
// starts with `<fieldname>=`.
//
// I/O errors are propagated to the caller.
//
// Rows that appear to be illegal on input are silently dropped.

package storage

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"
	"strings"
)

// Given the (relative) name of a root directory, a start date, a date past the end date, and a glob
// pattern, find and return all files that match the pattern in the data store, filtering by the
// start date.
//
// The path shall be a clean, absolute path that ends in `/` only if the entire path is `/`.
//
// For the dates, only year/month/day are considered, and timestamps should be passed as UTC times
// with hour, minute, second, and nsec as zero.
//
// The pattern shall have no path components and is typically a glob

func EnumerateFiles(data_path string, from time.Time, to time.Time, pattern string) ([]string, error) {
	filesys := os.DirFS(data_path)
	result := []string{}
	for from.Before(to) {
		probe_fn := fmt.Sprintf("%4d/%02d/%02d/%s", from.Year(), from.Month(), from.Day(), pattern);
		matches, err := fs.Glob(filesys, probe_fn)
		if err != nil {
			return nil, err
		}
		result = append(result, matches...)
		from = from.AddDate(0, 0, 1)
	}
	return result, nil
}
	
// General "free CSV" reader, returns array of maps from field names to field values.

func ReadFreeCSV(path string) ([]map[string]string, error) {
	input_file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	input := bufio.NewReader(input_file)
	rdr := csv.NewReader(input)
	// Rows arbitrarily wide, and possibly uneven.
	rdr.FieldsPerRecord = -1
	rows := make([]map[string]string, 10)
	for {
		fields, err := rdr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// TODO: Something more subtle?  Distinguish I/O error from decoding error?
			return nil, err
		}
		m := make(map[string]string)
		for _, f := range(fields) {
			ix := strings.IndexByte(f, '=')
			if ix == -1 {
				// Illegal syntax, just drop the field.
				continue
			}
			m[f[:ix]] = f[ix+1:]
		}
	}
	input_file.Close()
	return rows, nil
}

// General "free CSV" writer.  The fields that are named by `fields` will be written, if they exist
// in the map (otherwise nothing is written for the field).  The fields are written in the order
// given.

func WriteFreeCSV(path string, fields []string, data []map[string]string) error {
	output_file, err := os.CreateTemp(path, "naicreport-csvdata")
	if err != nil {
		return err
	}
	wr := csv.NewWriter(output_file)
	for _, row := range data {
		// TODO: With go 1.21, we can hoist this and clear() it after the write, instead of
		// reallocating each time through the loop.
		r := []string{}
		for _, field_name := range fields {
			if field_value, present := row[field_name]; present {
				r = append(r, field_name + "=" + field_value)
			}
		}
		if len(r) > 0 {
			wr.Write(r)
		}
	}
	wr.Flush()
	output_file.Close()
	os.Rename(output_file.Name(), path)
	return nil
}
