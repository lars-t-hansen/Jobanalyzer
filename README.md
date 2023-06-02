# Cross system Jobanalyzer

Easy-to-use resource usage reporting and analyses.

## Sample use cases

The use cases span a usage spectrum from "pure production" to "partly
development" to "systems administration":

* (Automatic monitoring and offloading) User X runs a job on an ML
  node but the job does not use the GPUs, yet X's CPU usage is such
  that other users who could use the GPUs do not use the machine
  because the CPUs are overloaded.  X should move to a non-GPU system
  such as Fox, but user X is unaware of the problem.  X or admins
  should be alerted to the problem so that X can be made to move.

* (Manual monitoring) Admin Y wants to view the current load of a
  shared server.

* (Manual monitoring) Admin Y wants to view historical utilization
  data of a shared server.

* (Development and debugging) User X runs a analysis using Pytorch. X
  expects the code to use GPUs. X wants to check that the code did
  indeed use the GPU during the last 10 analyses that ran to
  completion.

* (Development and debugging) User X submits a HPC job expecting to
  use 16 cores and 8GB memory per CPU. Admins complain that X is
  wasting resources (the program runs on one core and uses 4GB). In
  order to debug the problem, X want to check how much resources the
  job just finished used.

* (Development and debugging) User X wants to understand a (say)
  matrix multiplication program written in C++ with an eye to whether
  it will scale to larger systems.

In principle, the hardware spans the spectrum: personal systems, ML
nodes, Fox, national systems, maybe even LUMI.

Until we decide otherwise we're going to assume that the program may
assume that the systems we're running on are "healthy".  There should
be other software in place to determine if the systems are indeed
healthy.

## Non-use cases (probably)

* User X is developing new code and sitting at the terminal and wants
  to view GPU, CPU, and memory usage for the application, which is
  running.  For this X can already use `nvtop`, `nvitop`, `htop`, and
  similar applications.

* In general, traditional "profiling" use cases during development
  (finding hotspots in code, etc) are out of bounds for this project.

## General discussion

* For several use cases above the only plausible solution is some type
  of continuous log.  There are some concerns with GDPR/personvern as
  well as security.  A meaningful log would necessarily contain a
  history of runs, keyed by UID and time, and probably at least part
  of the command line for a process.  Thus the user's activities may
  be tracked without consent, and secrets divulged on the command line
  may be exposed.  See SKETCHES.md for more.

* The main use case is for jobs that run (or ran) "for a while", that
  is, more than a few seconds at least, possibly the bar is set
  higher.  (For reference, a 20,000 x 20,000 matrix multiply runs in
  about 10s on a 2080 card; that task probably would not qualify.)  At
  the same time, one use case asks for "historical usage statistics".
  It's open whether those statistics also include smaller jobs.

* Several use cases above compare the consumed resources to the
  (explicitly or implicitly) requested resources, or to the available
  resources.  Thus, on systems where it makes sense the log must also
  contain the requested resources.  On ML nodes with expensive GPUs,
  the GPUs are implicitly requested.  For scalability analyses, if a
  program can't make use of the machine it's running on then it's not
  going to help moving it to a larger system.

* We don't want to be tied to systems that do or don't have work
  queues.

## Survey of existing tools

* There are good profilers already, but generally you need to
  commision the profile when the job starts, and sometimes you must
  rebuild the code for profiling.  Traditional profilers do not speak
  to most of the use cases.

* Some job monitors may do part of the job, for example, `nvtop` will
  show GPU load and process ID and gives a pretty clear picture of
  whether the job is running on the GPU.  (Like `htop` and `top` for
  the CPU.)  These monitors can be started after the job is started.

* `nvtop` also works on AMD cards, though only for kernel 5.14 and
  newer.  There is also https://github.com/clbr/radeontop which I have
  yet to look at.

* `nvidia-smi` can do logging and is possibly part of the solution to
  generating the log.  See SKETCHES.md for more.

* `rocm-smi` may have some similar capabilities for the AMD cards.

* The code that creates the load dashboard on ML nodes is
  [here](https://github.uio.no/ML/dashboard) and may be part of the
  solution.

* Sigma2 uses RRD but this is more a database manager and display tool
  than anything else.

* We have something running called Zabbix that is used to monitor
  health and performance but I don't know how this works or what it
  does.

* Open XDMod seems like a comprehensive tool but may be
  queue-oriented.

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

## Solution sketch

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
database.  There are at least two ways of viewing the database:

In one view, it is a sequential event log with occasional
consolidation, very cheap event recording but a fairly expensive
processing step (the entire thing has to be read and processed).

In the other view, it is a map from PID (which we consider unique for
the sake of argument) to information about the PID's process.  Sample
recording is more complicated; many records may have to be updated.
And it is necessary to also keep track of processes that were alive
but now are not, so that their records can be sealed.

This second view is possibly more useful if we are concerned not about
time, but about how individual jobs used the resources of the system.

## The trickiness of rules

The "Automatic monitoring and offloading" case is harder than all the
others because, "automatic".  What does it mean for a job to be using
a "lot" of CPU and a "little" GPU?

Consider a machine like ml6 which appears to have 64 (hyperthreaded)
CPU cores, 256GB of RAM, and eight RTX 2080 Ti cards each with 10GB
VRAM.

Which of these scenarios do we care about?

* A job runs flat-out on a single CPU for a week, it uses 4GB RAM and
  no GPU. (We prefer it to move to Fox but we don't care very much,
  *unless* there are many of these.)

* A job runs flat-out on 16 cores for a week, it uses 32GB of RAM and
  no GPU. (We really want this to move to Fox.)

* Like the one-CPU case, but it also uses one GPU for most of that
  time.  (I have no idea.)

* Like the 16-CPU case, but it also uses one GPU for most of that
  time.  (I have no idea.)

* Like the 16-CPU case, but it also uses several GPUs for most of that
  time.  (It stays on ML6, unless it's using a lot of doubles on the
  GPUs, in which case it should maybe move to ML8 with the A100s?)


## Solution tech

Normally for this type of thing one would use Go, which is designed
for it.  It may have portability issues to the various systems that we
target, however.  It's not installed on the ML nodes, but we could fix
this: Fox has go 1.14; Saga has go 1.17 and 1.18.

EasyBuild is itself written in Python with PyPI/pip, which suggests
using that stack would be the path of least resistance, modulo the
dependency hell.  The lack of static types is a fairly serious
liability.  But most sysadmins should be able to relate to it.

(We should consider bash/awk completely out of bounds for anything
more than a few lines of code.)

C++ is probably a candidate, all things considered, but requires more
specialized maintainer knowledge.
