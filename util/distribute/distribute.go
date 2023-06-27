// This command line utility reads an input file of text lines and distributes N blocks of
// consecutive lines into files in the provided N directories.  The files in the directories are
// named the same as the base name of the input file.
//
// Usage:
//
//   distribute filename dir1 ...
//
// TODO: A better realization of this program would read the log records and place each record in a
// file in a directory that is appropriate for it.

package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
)

func main() {
	as := os.Args
	if len(as) < 3 {
		fail("Usage: distribute filename dir ...")
	}
	infilename := as[1]
	dirs := as[2:]

	infile, err := os.OpenFile(infilename, os.O_RDONLY|os.O_APPEND, 0)
	check(err, "Error opening %v: %v\n", infilename, err)

	// Count lines and compute number of lines per output file
	lines := 0
	{
		rdr := bufio.NewReader(infile)
		for {
			_, err := rdr.ReadString('\n')
			if err == io.EOF {
				// Subtle bug: non-LF terminated lines are not handled properly I think
				break
			}
			check(err, "Error reading %v: %v\n", infilename, err)
			lines++
		}
		_, err := infile.Seek(0, 0)
		check(err, "Error seeking %v: %v\n", infilename, err)
	}
	num_per_file := (lines + (len(dirs) - 1)) / len(dirs)

	// Populate the directories
	rdr := bufio.NewReader(infile)
	for _, dir := range dirs {
		outfilename := dir + "/" + path.Base(infilename)
		outfile, err := os.Create(outfilename)
		check(err, "Error creating %v: %v\n", outfilename, err)
		writer := bufio.NewWriter(outfile)
		for i := 0; i < num_per_file; i++ {
			s, err := rdr.ReadString('\n')
			if err == io.EOF {
				// Subtle bug: non-LF terminated lines are not handled properly I think
				break
			}
			check(err, "Error reading %v: %v\n", infilename, err)
			_, err = writer.WriteString(s)
			check(err, "Error writing to %v: %v\n", outfilename, err)
		}
		writer.Flush()
		outfile.Close()
	}
}

func check(err error, msg string, irritant ...any) {
	if err != nil {
		fmt.Fprintf(os.Stderr, msg, irritant...)
		os.Exit(1)
	}
}

func fail(msg string, irritant ...any) {
	fmt.Fprintf(os.Stderr, msg, irritant...)
	os.Exit(1)
}
