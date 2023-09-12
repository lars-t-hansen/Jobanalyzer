// Generate data for plotting the running load of the ML systems.  The data are taken from the live
// sonar logs, by means of sonalyze.

// Rough design:
//
// - run sonalyze for some time range and capture the desired output
// - parse the output into an internal form
// - generate plottable data
// - emit plottable data to a file
// - somehow signal that the file has been updated (eg by git-commit)

package mlwebload

func MlWebload(progname string, args []string) error {
	// We need a data-path and from/to
	//  --data-path
	//  --from
	//  --to
	// We ideally also need the path for `sonalyze` if not in the path (can use LookPath)
	//  --sonalyze
	// run `sonalyze load` on those arguments with a format string and csv output
	// use os/exec for all of that
	// we want to be able to plot each system individually
	// what do we plot?
	//  sonalyze load --from=2d --hourly --fmt=csvnamed,date,time,cpu,mem,gpu,gpumem,rcpu,rmem,rgpu,rgpumem,gpus
	// Need to fix #51, we want records even when zero
	// For this report we *definitely* want host in the record, not between the records.  So fix that first, #67
	// For this we also want datetime, to simplify parsing - either straight with a space, or iso, #68
	// Define the output format based on Sabry's prototype
	// Figure out json encoding, probably
	// Discuss whether to plot everything together in one plot, or have separate plots - maybe this is all
	//  in some sort of web front end
	return nil
}
