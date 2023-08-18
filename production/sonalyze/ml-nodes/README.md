These are scripts and configuration files for running sonar and sonar
analyses on the ML nodes.  These should be located, together with
executables for sonar and sonalyze, in a sonar root directory, call
this $SONAR_ROOT.

The way this is supposed to be set up is that sonar.cron is installed
on all machines except ml4, and sonar-ml4.cron is installed on ml4.
The latter script runs analyses in addition to sonar.

Sonar runs every 5 minutes and logs data in $SONAR_ROOT/data/, under
which there is a tree with a directory for each year, under that
directories for each month, and under each month directories for each
day.  Directories are created as necessary.  In each leaf directory
there are csv files named by hosts (eg, `ml8.hpc.uio.no.csv`),
containing the data logged by sonar on that host on that day.

TODO:

- the scripts here are probably tied to larstha's home directory,
  this is fixable
