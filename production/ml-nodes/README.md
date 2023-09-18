# ML-nodes sonar and sonalyze setup

## Files

These are files that are used to drive sonar and the analysis of sonar
logs during production.

The work is driven by cron, so there are two crontabs:

- jobanalyzer.cron is a user crontab to run on each host, it is run on
  the ML nodes other than ML4.

- jobanalyzer-ml4.cron is run on ML4, and does more work (ML4 because
  nobody uses that node much).

The crontabs just run a bunch of shell scripts:

- sonar.sh is a script that runs sonar with a set of predetermined
  command line switches and with stdout piped to a predetermined
  location.

- cpuhog.sh and bughunt.sh are analysis jobs that process the sonar
  logs and look for jobs that either should not be on the ML nodes or
  are stuck and indicate system problems.

The analyses needs to know what the systems look like, so there are
files for that:

- ml-nodes.json describes the hardware of the ML nodes

## Production

The typical case in production will be that all of these files are
manually copied into a directory shared among all the ML nodes, called
`$HOME/sonar`, for whatever user is running these jobs.  Also in that
directory must be binaries for `sonar` and `sonalyze`.

The directory `$HOME/sonar/data` will appear when the jobs are run and
will contain the raw sonar logs as well as the output from the
analyses.

If your case is not typical you will need to edit the shell scripts
(to get the paths right and make any other adjustments).
