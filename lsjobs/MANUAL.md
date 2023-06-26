# `lsjobs` manual

## USAGE

List jobs for user in sonar logs.

### Summary

```
lsjobs [options] [-- logfile ...]
```

### Overall operation

The program operates by computing a set of input logs, reading these logs with input filters
applied, aggregating data across the remaining records, and then printint output with output
filters applied.

### Log file computation options

`--data-path=<path>`

  Root directory for log files, overrides the default.  The default is the `SONAR_ROOT` environment
  variable, or if that is not defined, `$HOME/sonar_logs`.

`-- <filename>`

  If present, each `filename` is used for input instead of anything reachable from the data path;
  the data path is ignored.

### Input filter options

`-u <username>`
`--user=<username>`

  The user name.  The default is the current user, `$LOGNAME`.  Use `-` for everyone.

`--exclude=<username>,...`

  Normally, users `root` and `zabbix` are excluded from the report.  (They don't run jobs usually,
  but with synthesized jobs they can appear in the log anyway.)  With the exclude option, list
  *additional* user names to be excluded.

`--job=<job#>`

  Select a specific job by job number.

`-f <fromtime>`
`--from=<fromtime>`

  Use only records with this time stamp and later, format is either `yyyy-mm-dd` or `start`, the
  latter signifying the first record in the logs. The default is 24 hours ago.

`-t totime`
`--to=...`

  Use only records with this time stamp and earlier, format is either `yyyy-mm-dd` or `end`, the
  latter signifying the last record in the logs.  The default is now.

`--host=<hostname>,...`

  Use only records with these host names.  The host name filter applies both to file name filtering
  in the data path and to record filtering within all files processed (as all records also contain
  the host name).

### Output filter options

`-n <number-of-records>`
`--numrecs=<number-of-records>`

  Show only the *last* `number-of-records` records per user.

`--avgcpu=<pct>`

  Show only jobs that have at least `pct` percent (an integer, one full CPU=100) average CPU utilization.

`--maxcpu=<pct>`
  Show only jobs that have at least `pct` percent (an integer, one full CPU=100) peak CPU utilization.

`--avggpu=<pct>`
`--maxgpu=<pct>`

   As for CPU, but for GPU.  Note that most programs use no more than one accelerator card, and
   there are fewer of these than CPUs, so these numbers will be below 100 for most jobs.

`--minrun=<time>`

   Show only jobs that ran for at least the given amount of time.  Time is given on the formats
   `DdHhMm` where the `d`, `h`, and `m` are literal and `D`, `H`, and `M` are nonnegative integers,
   all three parts - days, hours, and minutes -- are optional but at least one must be present.

## EXAMPLES

List my jobs for the last 24 hours with default filtering:

```
  lsjobs
```

## LOG FILES

The log files under the log root directory are expected to be in a directory tree coded first by
year (CE), then by month (1-12), then by day (1-31), with a file name that is the name of a host
with the ".csv" extension.  That is, `$SONAR_ROOT/2023/06/26/deathstar.hpc.uio.no.csv` could be such
a file.


## OUTPUT FORMAT

The basic listing format is
```
job-id  user running-time start-time end-time command type cpu gpu
```
where:

* `job-id` is a number possibly followed by a mark "!" (running at the start and end of the time interval),
  "<" (running at the start of the interval), ">" (running at the end of the interval).
* `user` is the user name
* `running-time` on the format DDdHHhMMm shows the number of days DD, hours HH and minutes MM the job ran for.
* `start-time` and `end-time` on the format `YYYY-MM-DD HH:MM` are the endpoints for the job
* `command` is the command name, as far as is known
* `type` is `gpu` if it used GPUs or blank otherwise
* `cpu` and `gpu` on the form `avg/max` show CPU an GPU utilization where 1.0 corresponds to one
   full core or device, ie on a system with 64 CPUs the CPU utilization can reach 64.0 and on a
   system with 8 accelerators the GPU utilization can reach 8.0.

Output records are sorted in order of increasing start time of the job.
