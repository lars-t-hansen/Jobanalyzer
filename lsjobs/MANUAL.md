# `lsjobs` manual

## USAGE

List jobs for user in sonar logs.

### Summary

```
lsjobs [options] [-- logfile ...]
```

### Overall operation

The program operates by phases:

* computing a set of input log files
* reading these log files with input filters applied, resulting in a set of input records
* aggregating data across the input records
* filtering the aggregated data with the aggregation filters
* printing the aggregated data with the output filters


### Log file computation options

`--data-path=<path>`

  Root directory for log files, overrides the default.  The default is the `SONAR_ROOT` environment
  variable, or if that is not defined, `$HOME/sonar_logs`.

`-- <filename>`

  If present, each `filename` is used for input instead of anything reachable from the data path;
  the data path is ignored.

### Input filter options

All filters are optional.  Records must pass all specified filters.

`-u <username>,...`, `--user=<username>,...`

  The user name(s).  The default is the current user, `$LOGNAME`.  Use `-` for everyone.

`--exclude=<username>,...`

  Normally, users `root` and `zabbix` are excluded from the report.  (They don't run jobs usually,
  but with synthesized jobs they can appear in the log anyway.)  With the exclude option, list
  *additional* user names to be excluded.

`-j <job#>,...`, `--job=<job#>,...`

  Select specific jobs by job number(s).

`-f <fromtime>`, `--from=<fromtime>`

  Select only records with this time stamp and later, format is either `YYYY-MM-DD`, `Nd` (N days ago)
  or `Nw` (N weeks ago).  The default is `1d`: 24 hours ago.

`-t <totime>`, `--to=<totime>`

  Select only records with this time stamp and earlier, format is either `YYYY-MM-DD`, `Nd` (N days
  ago) or `Nw` (N weeks ago).  The default is now.

`--host=<hostname>,...`

  Select only records from these host names.  The host name filter applies both to file name
  filtering in the data path and to record filtering within all files processed (as all records also
  contain the host name).  The default is all hosts.

### Aggregation filter options

All filters are optional.  Records must pass all specified filters.

`--min-avg-cpu=<pct>`

  Select only jobs that have at least `pct` percent (an integer, one full CPU=100) average CPU utilization.

`--min-peak-cpu=<pct>`

  Select only jobs that have at least `pct` percent (an integer, one full CPU=100) peak CPU utilization.

`--min-avg-mem=<size>`

  Select only jobs that have at least `size` gigabyte average main memory utilization.

`--min-peak-mem=<size>`

  Select only jobs that have at least `size` gigabyte peak main memory utilization.

`--min-avg-gpu=<pct>`

  Select only jobs that have at least `pct` percent (an integer, one full device=100) average GPU
  utilization.  Note that most programs use no more than one accelerator card, and there are fewer
  of these than CPUs, so this number will be below 100 for most jobs.
   
`--min-peak-gpu=<pct>`

  Select only jobs that have at least `pct` percent (an integer, one full device=100) peak GPU utilization.

`--min-avg-vmem=<pct>`

  Select only jobs that have at least `pct` percent (an integer, one full device=100) average GPU
  memory (video memory) utilization.

`--min-peak-vmem=<pct>`

  Select only jobs that have at least `pct` percent (an integer, one full device=100) peak GPU
  memory (video memory) utilization.

`--min-runtime=<time>`

  Select only jobs that ran for at least the given amount of time.  Time is given on the formats
  `DdHhMm` where the `d`, `h`, and `m` are literal and `D`, `H`, and `M` are nonnegative integers,
  all three parts - days, hours, and minutes -- are optional but at least one must be present.

`--no-gpu`

  Select only jobs that did not use any GPU.

`--some-gpu`

  Select only jobs that did use some GPU (even if the GPU avg/max statistics round to zero).

### Output filter options

`-n <number-of-records>`, `--numrecs=<number-of-records>`

  Show only the *last* `number-of-records` records per user.  The default is "all".  Output records
  are sorted ascending by the start time of the job, so this option will select the last started jobs.

## EXAMPLES

List my jobs for the last 24 hours with default filtering:

```
lsjobs
```

List the jobs for all users from up to 2 weeks ago in the given log file (presumably containing data
for the entire time period) that used at least 10 cores worth of CPU on average and no GPU:

```
lsjobs --user=- --from=2w --min-avg-cpu=1000 --no-gpu -- ml8.hpc.uio.no.csv
```

## LOG FILES

The log files under the log root directory -- ie when log file names are not provided on the command
line -- are expected to be in a directory tree coded first by four-digit year (CE), then by month
(1-12), then by day (1-31), with a file name that is the name of a host with the ".csv" extension.
That is, `$SONAR_ROOT/2023/6/26/deathstar.hpc.uio.no.csv` could be such a file.


## OUTPUT FORMAT

The basic listing format is
```
job-id  user running-time start-time end-time cpu main-mem gpu gpu-mem command 
```
where:

* `job-id` is a number possibly followed by a mark "!" (running at the start and end of the time interval),
  "<" (running at the start of the interval), ">" (running at the end of the interval).
* `user` is the user name
* `running-time` on the format DDdHHhMMm shows the number of days DD, hours HH and minutes MM the job ran for.
* `start-time` and `end-time` on the format `YYYY-MM-DD HH:MM` are the endpoints for the job
* `cpu`, `gpu`, and `gpu-mem` on the form `avg/max` show CPU, GPU, and video memory utilization as
   percentages, where 100 corresponds to one full core or device, ie on a system with 64 CPUs the
   CPU utilization can reach 6400 and on a system with 8 accelerators the GPU utilization and GPU
   memory utilization can both reach 800.
* `main-mem` on the form `avg/max` shows main memory average and peak utilization in GB
* `command` is the command name, as far as is known

Output records are sorted in order of increasing start time of the job.
