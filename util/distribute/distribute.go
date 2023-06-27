// This command line utility reads an input file of text lines and distributes N blocks of
// consecutive lines into files in the provided N directories.  The files in the directories are
// named the same as the base name of the input file.
//
// Usage:
//
//   distribute filename dir1 ...

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
		fmt.Fprintln(os.Stderr, "Usage: distribute filename dir ...")
		os.Exit(1)
	}
	infilename := as[1]
	dirs := as[2:]

	infile, err := os.OpenFile(infilename, os.O_RDONLY|os.O_APPEND, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening %v: %v\n", infilename, err)
		os.Exit(1)
	}

	// Count lines and compute number of lines per output file
	lines := 0
	{
		rdr := bufio.NewReader(infile)
		for {
			_, err := rdr.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					break
				}
				fmt.Fprintf(os.Stderr, "Error reading %v: %v\n", infilename, err)
				os.Exit(1)
			}
			lines++
		}
		_, err := infile.Seek(0, 0)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error seeking %v: %v\n", infilename, err)
			os.Exit(1)
		}
	}
	num_per_file := (lines + (len(dirs) - 1)) / len(dirs)

	// Populate the directories
	rdr := bufio.NewReader(infile)
	for _, dir := range dirs {
		outfilename := dir + "/" + path.Base(infilename)
		outfile, err := os.Create(outfilename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating %v: %v\n", outfilename, err)
			os.Exit(1)
		}
		writer := bufio.NewWriter(outfile)
		for i := 0 ; i < num_per_file ; i++ {
			s, err := rdr.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					fmt.Fprintf(os.Stderr, "Error reading %v: %v\n", infilename, err)
					os.Exit(1)
				}
				break
			}
			_, err = writer.WriteString(s)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error writing to %v: %v\n", outfilename, err)
				os.Exit(1)
			}
		}
		writer.Flush()
		outfile.Close()
	}
}
