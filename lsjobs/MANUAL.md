# `lsjobs` manual

## USAGE

Analyze sonar logs and print information about jobs or systems.

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

The default is that the program prints information about jobs, collected from the input records.
The `--load` switch instead instructs it to print information about the load on the systems in the
logs.

### Major operation options

`--load=<what>`

  Instead of printing information about jobs the program will print information about the load on
  the systems.  The `what` is `all` (print load at each recorded instant separately), `last` (print
  only the load at last instant in the selected records), `hourly` (aggregate data in hourly buckets
  and print hourly averages), or `daily` (ditto daily buckets).
  
  See `--loadfmt` for how to format the output.

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

  The user name(s).  The default is the current user, `$LOGNAME`, except in the case of `--load`,
  when the default is everyone.  Use `-` for everyone.

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
  `WwDdHhMm` where the `w`, `d`, `h`, and `m` are literal and `W`, `D`, `H`, and `M` are nonnegative
  integers, all four parts - weeks, days, hours, and minutes -- are optional but at least one must
  be present.  (Currently the parts can be in any order but that may change.)

`--no-gpu`

  Select only jobs that did not use any GPU.

`--some-gpu`

  Select only jobs that did use some GPU (even if the GPU avg/max statistics round to zero).

`--completed`

  Select only jobs that have completed (have no samples at the last time recorded in the log).

`--running`

  Select only jobs that are still running (have a sample at the last time recorded in the log).

`--zombie`

  Select only jobs deemed to be zombie jobs.

`--command=<command>`

  Select only jobs whose command name contains the `<command>` string.  This is a little ambiguous,
  as a job may have more than one process and not all processes need have the same command name.
  For this filtering, as for the output, select the name of the process with the earliest recorded
  start time.

### Output filter options

`-n <number-of-jobs>`, `--numjobs=<number-of-jobs>`

  Show only the *last* `number-of-jobs` selected jobs per user.  The default is "all".  Selected
  jobs are sorted ascending by the start time of the job, so this option will select the last
  started jobs.

`--loadfmt=<format>`

  Format the output for `--load` according to `format`, which is a comma-separated list of keywords:
  `date` (`YYYY-MM-DD`), `time` (`HH:MM`), `datetime` (combines `date` and `time`), `cpu` (percentage,
  100=1 core), `mem` (GB), `gpu` (percentage, 100=1 card), `vmem` (two fields, GB and percent, these
  are unreliable in different ways on different systems), `gpus` (bitmap).

## COOKBOOK

These relate mostly to the use cases in [../README.md](../README.md).

### Is the system being used appropriately?

Use case: jobs running on the ML nodes that use a lot of CPU but little or no GPU should not be
there; they should generate alerts.

This is not yet automated, but for some manual monitoring try the following.  It lists the jobs for
all users from up to 2 weeks ago that used at least 10 cores worth of CPU on average and no GPU and ran
for at least 15 minutes:

```
lsjobs --user=- --from=2w --min-avg-cpu=1000 --no-gpu --min-runtime=15m
```

### Are there zombie jobs on the system?

Use case: there should be no zombie jobs; zombie jobs should generate alerts.

This is not yet automated, and it is evolving (and is hard to test) but if Sonar does zombie
detection right then the following should work.  (Zombie jobs tend to stick around forever once they
reach that state, so `--running` isn't necessary).

```
lsjobs --from=2w --zombie
```

### What is the current utilization of the host?

Use case: We want to know how much the system is loaded by currently running long-running jobs.

```
lsjobs --load=last
```

### What is the historical utilization of the host?

Use case: We want to know how much the system has been loaded by long-running jobs, over time.

Here's the daily average CPU and GPU utilization for the last year.  (Hourly averages may be more
meaningful but would create too much data for the year.)

```
lsjobs --from=1y --load=daily --loadfmt=cpu,gpu
```

Note these are "absolute" values in the sense that, though they are percentages, the reference for
100% is one CPU core or GPU card.  If you instead want values relative to the system, you need to
ask for that, and you need to provide the system configuration:

```
lsjobs --from=3d --load=hourly --loadfmt=rcpu,rgpu --config-file=ml-systems.json
```

### Did my job use GPU?

Use case: Development and debugging, check that the last 10 pytorch jobs used GPU as they should.
Run:

```
lsjobs --command=python --numjobs=10 --completed
```

and then inspect the fields for `gpu` and `gpu mem`, which should be nonzero.

(TODO: There are some obscure cases in which it is possible for these fields to be zero yet
`--some-gpu` would select the records; this seems related to some memory reservations that are not
accounted for in the memory usage numbers.)

### What resources did my job use?

Use case: Development and debugging, list the resource usage of my last completed job.  Run:

```
lsjobs --numjobs=1 --completed
```

### Will my program scale?

Use case: Will my program that I just ran scale to a larger system?  Run

```
lsjobs --numjobs=1 --completed
```

and consider resource utilization relative to the system the job is running on.  If requested GPU
and CPU resources are not maxed out then the program is not likely to scale.

## OTHER EXAMPLES

List all my jobs the last 24 hours:

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


## SYSTEM CONFIGURATION FILES

The system configuration files are JSON files providing the details for each host.

(To be documented.)

## OUTPUT FORMAT

### Jobs

The basic job listing format is
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
* `command` is the command name, as far as is known.  For jobs with multiple processes that have different
   command names, choose the name of the process with the earliest recorded start time.

Output records are sorted in order of increasing start time of the job.

### Systems

The output can be controlled with `--loadfmt`.  The default output format is
`datetime,cpu,mem,gpu,vmem,gpus`.  Unless a single host is explicitly selected with `--host` then
the host name is printed on a separate line before the data for the host.
