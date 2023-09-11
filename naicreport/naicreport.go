// Run `naicreport help` for help.

package main

import (
	"fmt"
	"naicreport/mlcpuhog"
	"os"
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
		err = err
		// TODO:
		//  - usage error
		//  - other error

	default:
		toplevelUsage(1)
	}
}

func toplevelUsage(code int) {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n")
	fmt.Fprintf(os.Stderr, "  %s <verb> <option> ...\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "where <verb> is one of\n")
	fmt.Fprintf(os.Stderr, "  help\n")
	fmt.Fprintf(os.Stderr, "    Print help\n")
	fmt.Fprintf(os.Stderr, "  ml-cpuhog\n")
	fmt.Fprintf(os.Stderr, "    Analyze the cpuhog logs and generate a report of new violations\n")
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "All verbs accept -h to print verb-specific help\n")
	os.Exit(code)
}
