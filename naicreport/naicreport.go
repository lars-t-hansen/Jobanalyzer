// Run `naicreport help` for help.

package main

import (
	"fmt"
	"os"

	"naicreport/mlcpuhog"
	"naicreport/mlwebload"
)

func main() {
	if len(os.Args) < 2 {
		toplevelUsage(1);
	}
	var err error
	switch os.Args[1] {
	case "help":
		toplevelUsage(0)

	case "ml-cpuhog":
		err = mlcpuhog.MlCpuhog(os.Args[0], os.Args[2:])

	case "ml-webload":
		err = mlwebload.MlWebload(os.Args[0], os.Args[2:])

	default:
		toplevelUsage(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n\n", err)
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
	fmt.Fprintf(os.Stderr, "    Analyze the cpuhog logs and generate a report of new violations\n\n")
	fmt.Fprintf(os.Stderr, "  ml-weblog\n")
	fmt.Fprintf(os.Stderr, "    Run sonalyze to generate a plottable load report\n\n")
	fmt.Fprintf(os.Stderr, "All verbs accept -h to print verb-specific help\n")
	os.Exit(code)
}
