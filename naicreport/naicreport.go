// Run `naicreport help` for help.

package main

import (
	"fmt"
	"os"

	"naicreport/mlcpuhog"
)

func main() {
	if len(os.Args) < 2 {
		toplevelUsage(1);
	}
	switch os.Args[1] {
	case "help":
		toplevelUsage(0)

	case "ml-cpuhog":
		err := mlcpuhog.MlCpuhog(os.Args[0], os.Args[2:])
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n\n", err)
			toplevelUsage(1)
		}

	default:
		toplevelUsage(1)
	}
}

func toplevelUsage(code int) {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <verb> <option> ...\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "where <verb> is one of\n\n")
	fmt.Fprintf(os.Stderr, "  help\n")
	fmt.Fprintf(os.Stderr, "    Print help\n\n")
	fmt.Fprintf(os.Stderr, "  ml-cpuhog\n")
	fmt.Fprintf(os.Stderr, "    Analyze the cpuhog logs and generate a report of new violations\n")
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "All verbs accept -h to print verb-specific help\n")
	os.Exit(code)
}
