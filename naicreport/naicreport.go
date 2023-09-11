// Run `naicreport help` for help.

package main

import (
	"flag"
	"fmt"
	"os"
)

type CommonArgs struct {
}

func main() {
	common_args, operation := parse_command_line()
	switch e := operation.(type) {
	case *MlCpuhogOp:
		MlCpuhog(common_args, e)

	default:

	}
}

func parse_command_line() (*CommonArgs, any) {
	if len(os.Args) < 2 {
		toplevelUsage(1);
	}
	switch os.Args[1] {
	case "help":
		toplevelUsage(0)

	case "ml-cpuhog":
		opts := flag.NewFlagSet(os.Args[0] + " ml-cpuhog", flag.ExitOnError);
		data_path := opts.String("data-path", "", "Root directory of data store (required)")
		from := opts.String("from", "1d", "Start of log window")
		opts.Parse(os.Args[2:])
		if *data_path == "" {
			fmt.Fprintf(os.Stderr, "-data-path requires a value\nUsage of %s ml-cpuhog:\n", os.Args[0])
			opts.PrintDefaults()
			os.Exit(1)
		}
		// TODO: data_path must be cleaned up:
		// - must be absolute
		// - must be Cleaned according to path
		return &CommonArgs {
				DataPath: *data_path,
				From: *from,
			},
			&MlCpuhogOp { }

	default:
		toplevelUsage(1)
	}
	panic("Should not happen")
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
