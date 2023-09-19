# Jobanalyzer: Design and implementation

## Architectural overview

We use [`sonar`](https://github.com/NordicHPC/sonar) as a general system sampler.  Sonar runs
periodically (typically every 5m or so, by means of `cron`) on all interesting hosts, and indeed on
every node in a cluster; it samples the hosts' state when it runs, and writes raw sample data to
files in a directory tree.  These samples need careful postprocessing and additional context to make
much sense.

We use [`sonarlog`](sonarlog) (in this repository) to ingest, contextualize, enrich, tidy up, and
filter the Sonar logs.  Sonarlog produces "sample streams", which are clean, self-consistent, and
chronologically sensible per-process or per-process-cluster sample data.  Sonarlog runs as a component
of `sonalyze`, see next.

We use [`sonalyze`](sonalyze) (in this repository) to aggregate, merge, query, and format the sample
streams to present meaningful summaries of "jobs".  See [`sonalyze/MANUAL.md`](sonalyze/MANUAL.md)
for instructions about how to run it, and [`README.md`](README.md) for some sample use cases.

Part of the complexity in the system up to this level stems from its generality: it works on both
single-node and multiple-node systems, on systems with or without a batch queue, and for jobs that
consist of a single command as well as those that consist of many different commands.

Anyway, there are many options available to Sonalyze to make it or Sonarlog select time windows,
sample records, jobs, and output formats.  See [`sonalyze/MANUAL.md`](sonalyze/MANUAL.md) or run
Sonalyze with `--help`.

Built on top of Sonalyze there are shell scripts that run periodically to run Sonalyze on the Sonar
logs and to produce further logs.  These scripts are system-specific; the ones for the UiO ML nodes
are in [`production/ml-nodes`](production/ml-nodes) and correspond in some cases directly to use cases
in [`README.md`](README.md).

Finally, there is a tool [`naicreport`](naicreport) that is a user-friendly superstructure for most
of the foregoing: it ingests the logs produced by analysis shell scripts, it runs Sonalyze directly,
and produces human-readable and machine-readable reports and data, for further incorporation in
emails, plots, and so on.


## Production setup

See [production/ml-nodes/README.md](production/ml-nodes/README.md) for instructions about how to set
up and run everything.


## Implementation overview

Sonar is written in Rust (a sensible choice, and one made some time ago).  Sonarlog is also written
in Rust, specifically so that it can be shared between Sonalyze and another tool,
[`jobgraph`](https://github.com/NordicHPC/jobgraph), which is itself written in Rust.  Since
Sonarlog is not standalone but is embedded into Sonalyze, Sonalyze is also written in Rust.

In contrast, Naicreport is written in Go for greater flexibility (the rigidity of Rust's ownership
system and manual memory management get in the way of getting things done).

Logic is pushed into Naicreport and Sonalyze when it is sensible and possible; the surrounding shell
scripts are kept very simple.


-----

The following is older / stale; please ignore for now.

## General discussion

* For several use cases above the only plausible solution is some type
  of continuous log.  There are some concerns with GDPR/personvern as
  well as security.  A meaningful log would necessarily contain a
  history of runs, keyed by UID and time, and probably at least part
  of the command line for a process.  Thus the user's activities may
  be tracked without consent, and secrets divulged on the command line
  may be exposed.

* The main use case is for jobs that run (or ran) "for a while", that
  is, more than a few seconds at least, possibly the bar is set much
  higher (minutes to hours or much more).  (For reference, a 20,000 x
  20,000 matrix multiply runs in about 10s on a 2080 card; that task
  probably would not and should not qualify.)  At the same time, one use case asks
  for "historical usage statistics".  It's open whether those
  statistics also include smaller jobs.

* Several use cases above compare the consumed resources to the
  (explicitly or implicitly) requested resources, or to the available
  resources.  Thus, on systems where it makes sense the log (or an
  accompanying log) must also contain the requested resources.  For example,

   * On ML nodes with expensive GPUs, the GPUs are implicitly requested.
   * For scalability analyses, if a program can't make use of the
     machine it's running on (the "implicitly requested resources")
     then it's not going to help moving it to a larger system.

* We don't want to be tied to systems that do or don't have work
  queues.

* It may be sensible for somebody with a short-running program to be
  able to request the logger to run locally with a short profiling
  interval (say for the sake of scalability analysis), even though
  this is close to being a non-use case.

* It's going to be an interesting problem to define a "job" on systems
  that don't have job queues, this is discussed further down.

## Survey of existing tools

* There are good profilers already, but generally you need to
  commision the profile when the job starts, and sometimes you must
  rebuild the code for profiling.  Traditional profilers do not speak
  to most of the use cases.

* Some job monitors may do part of the job, for example, `nvtop` will
  show GPU load and process ID and gives a pretty clear picture of
  whether the job is running on the GPU.  (Like `htop` and `top` for
  the CPU.)  These monitors can be started after the job is started.
  In fact, `nvtop` shows both GPU and CPU load and a quick peek at
  `nvtop` is often enough to find out whether a busy system is being
  used well.

* `nvtop` also works on AMD cards, though only for kernel 5.14 and
  newer.  (There is also https://github.com/clbr/radeontop which I have
  yet to look at.)

* `cat /proc/loadavg` gives a pretty good indication of the busyness
  of the CPUs over the last 15 minutes.

* `nvidia-smi` can do logging and is possibly part of the solution to
  generating the log.  See SKETCHES.md for more.

* `rocm-smi` may have some similar capabilities for the AMD cards.

* The `sonar` tool is roughly the right thing for basic data
  production, https://github.com/NordicHPC/sonar.  It can be
  augmented with functionality to extract GPU data and hunt for zombie
  processes using GPU memory.  (See larstha's clone
  of the repo for code that does that.)

* The `jobgraph` tool, augmented with a notion of what a "job" means on
  the ML and light-HPC systems, can be used to address the three "Development
  and debugging" use cases: it can take a job (or a set of jobs, with a little
  work) and display their resource consumption, which is what we want.  See https://github.com/NordicHPC/jobgraph.

* The code that creates the load dashboard on ML nodes is
  [here](https://github.uio.no/ML/dashboard) and may be part of the
  solution.

* Sigma2 uses RRD for some things but this is (apparently) more a
  database manager and display tool than anything else.

* We have something running called Zabbix that is used to monitor
  health and performance but I don't know how this works or what it
  does.

* Open XDMod seems like a comprehensive tool but may be dependent on
  having a job queue.

## Resources

Above, there's a discussion of CPU/GPU usage and memory usage, but the resource
landscape is broader and might include any of these:

* CPU (number in use; load; in principle also the features used, such as AVX512)
* GPU (number in use; load; in principle also the features used or the APIs, eg,
  using doubles vs floats)
* CPU/main memory (real memory occupancy, averages and peaks)
* GPU memory
* PCI bandwidth, maybe
* Disk bandwidth, maybe, esp writes
* Disk usage (scratch disk)
* Other kinds of bandwidth, maybe (other interconnects than PCI)
* Interactivity / response time is a kind of resource but unclear how that fits in

Some of these are easy to measure (CPU time); some are tricky (memory,
because memory is mapped, shared, cached, swapped, and so on); some
are unknown (bus/disk/interconnect bandwidth); and some are possibly
expensive (disk usage).

## Consumers

What is a "resource consumer", and what is a "job"?

If we have a job queue it's not too difficult - a job is what was
created by the queue manager (SLURM et al), and the resources
requested for the job were the resources outlined in the job script.

Absent a job queue it's harder:

A job is not something as simple as a PID, since even individual
threads have PIDs.  And it's not even something as simple as a
collection of threads that share kernel resources (memory map etc) and
is what Posix defines as a "process".

It's tempting to say that a "job" is a process tree that was started
from an interactive shell or login shell or ssh, though this runs into
some problems with interactive long-running tools such as Jupyter.
But as a first attempt it may be OK.  It captures the situation where
one process creates subprocesses to act on its behalf.  This includes
shell scripts that coordinate other programs, clearly.

The "resources requested" for this type of job are not so easy to
define.  For the ML nodes, there's an expectation (per the web page)
to use at most 1/4 of the (virtual!) CPUs and no more than the free
memory (clearly unbalanced, but that's what it says).  In addition
there's the expectation that "some" GPU will be used.  See below under
"The trickiness of rules" for more about this.


## Solution sketch [OBSOLETE]

All the use cases are really log-processing use cases, even the case
about a program scaling to a larger system.  Ergo we require

* Continuous logging of resource consumption, resource requests, and
  resource availability in a database

* Some type of data provider plugin architecture to cater to different
  types of systems

* Some type of consolidation of data over time (to control volume)

* A query/display interface against the database

* Possibly a way of authoring analysis rules that does not require
  writing actual code, but minimally a framework that rules can fit
  into easily.

* Some type of data consumer plugin architecture to cater to different
  types of analyses and reports, and different types of systems

Effectively it's a sample-based system profiler: at the time of each
sample, the system's state is recorded in some compact format in the
database.  There are at least three ways of viewing the database:

* In one view, it is a sequential event log with occasional
  consolidation, very cheap event recording but a fairly expensive
  processing/query step (the entire thing has to be read and
  processed).  It's not clear how costly it will be to process it
  repeatedly to look for trigger conditions.

* In a second view, it is a map from PID (really PID x creation-time
  since PIDs can be reused) to information about the PID's process.
  Sample recording and book-keeping is more complicated; many records
  may have to be updated every time the system is sampled.  Running
  rules is somewhat cheaper than the first view.

* In a third view, it is a map from UID to information about the
  user's jobs (where that information is probably a cluster of
  records, one for each PID).  This has even more complicated
  book-keeping than the second view and thus makes logging even more
  expensive, but makes information in the database more directly
  actionable.

The second and third views are possibly most useful if we are
concerned not about what happens along a timeline, but about how
individual jobs or individual users used the resources of the system.

On the other hand, some of the use cases are also about the timeline:
what is the current load, what was the historical load, what did my
last / 10 last jobs do?

Maybe the correct view is as an event stream with multiple consumers
that maintain databases that fit their needs.

## The trickiness of rules

The "Automatic monitoring and offloading" case is harder than all the
others because, "automatic".  What does it mean for a job to be using
a "lot" of CPU and a "little" GPU?

Consider a machine like ml6 which appears to have 32 (hyperthreaded)
CPU cores, 256GB of RAM, and eight RTX 2080 Ti cards each with 10GB
VRAM.

Which of these scenarios do we care about?

* A job runs flat-out on a single CPU for a week, it uses 4GB RAM and
  no GPU. (We prefer it to move to light-HPC/Fox but we don't care
  very much, *unless* there are many of these, possibly from many
  users.)

* A job runs flat-out on 16 cores for a week, it uses 32GB of RAM and
  no GPU. (We really want this to move to light-HPC/Fox.)

* Like the one-CPU case, but it also uses one GPU for most of that
  time.  (I have no idea.)

* Like the 16-CPU case, but it also uses one GPU for most of that
  time.  (I have no idea.)

* Like the 16-CPU case, but it also uses several GPUs for most of that
  time.  (It stays on ML6, unless it's using a lot of doubles on the
  GPUs, in which case it should maybe move to ML8 with the A100s?)

It may be that there needs to be a human in the loop: the system
generates an alert and the human (admin) can act on it or not by
alerting the user.  I guess in principle this is an interesting
machine learning problem.

## Solution tech

A possibility is that Zabbix can be used for the system, or part of
it.  At the very least it looks like it can be the agent for
communicating with the outside world, if it's not the agent for raw
data collection per se.  Zabbix can do mqtt and probably other queues.

Absent that:

Normally for this type of thing one would use Go, which is designed
for it.  It may have portability issues to the various systems that we
target, however.  It's not installed on the ML nodes or on eg
bioint01, but we could fix this: Fox has go 1.14; Saga has go 1.17 and
1.18.

EasyBuild is itself written in Python with PyPI/pip, which suggests
using that stack would be the path of least resistance, modulo the
dependency hell.  The lack of static types is a fairly serious
liability.  But most sysadmins should be able to relate to it.

(We should consider bash/awk completely out of bounds for anything
more than a few lines of code.)

C++ is probably a candidate, all things considered, but requires more
specialized maintainer knowledge.

Sonar is written in Rust; it's a bit low-level but would be fine
probably.

Assuming we limit ourselves to Linux, much info is available under
/proc.


