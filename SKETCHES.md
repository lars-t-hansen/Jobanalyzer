# Notes

These are very loose notes; ignore.

## Task breakdown

### Raw event collection

Initially probably this:

PID, PPID, UID, Wall time for the PID, CPU time used for the PID, GPU time used for the PID

From that, a more structured log (based around the UID and the user's
process tree) can be created I think.

What I really want is for this:

```
nvidia-smi --id=0 --query-gpu=utilization.gpu,utilization.memory,memory.total,memory.free,memory.used --format=csv --loop=1
```
which produces this:
```
0 %, 0 %, 11264 MiB, 11019 MiB, 0 MiB
71 %, 21 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
94 %, 33 %, 11264 MiB, 11019 MiB, 0 MiB
0 %, 0 %, 11264 MiB, 11019 MiB, 0 MiB
``
to also show PID if possible.  The info is there but 

Note, the NVIDIA cards have an "accounting mode" currently being
"disabled" on the systems I've checked.  This would likely be somewhat
useful for getting good data.  Otherwise we're going to have to just
get the PIDs allocated to the GPU and maybe divide the GPU utilization
among them, b/c, how can we do better?

Accounting mode can be turned on on boot-up and will persist if
persistent mode is on.  NVIDIA docs say that this does not impact performance.

We see things like these (after running with mmul):

```
$ nvidia-smi --query-accounted-apps=gpu_name,pid,time,gpu_util,mem_util,max_memory_usage --format=csv
gpu_name, pid, time [ms], gpu_utilization [%], mem_utilization [%], max_memory_usage [MiB]
NVIDIA A100-PCIE-40GB, 1906656, 14193 ms, 55 %, 8 %, 4989 MiB

$ echo $$
1905126
```

We may not be able to *assume* accounting, but it seems likely that we
could get better data this way.  So perhaps there are a couple
plugins, one for systems with accounting and one for systems without
(maybe AMD).

https://community.amd.com/t5/archives-discussions/monitoring-gpu-utilization/td-p/175782
https://linuxhint.com/apps-monitor-amd-gpu-linux/


Weekly meeting (Bart), this sounds interesting:
"cpu_hour_usage file generation for TSD's cost function."


## Requirements

These requirements spring from the use cases in README.md and from additional discussions.

### User view: Post-hoc view of "what happened with my job"

The user ran a job (or several jobs), maybe observed that they didn't perform well, and now wants to
find out why.

This is a blurry use case IMO but given that ML/HPC jobs are long-running, the ability to look
post-hoc at resource consumption at least gives the user a first indication of whether the job used
the available or requested resources.

This could be served by a logging resource monitor with some PID / UID selection facility and a
display facility.

### User view: How will my job scale?

This is a very different view on the same problem.  The scalability of a job may be inferred (in
some cases) from how it uses resources.  Say you're on a 64-core system and you observe that the job
uses only 8 cores.  It will likely not scale to a larger system.  Ditto for GPUs; if run on an 8-GPU
system but using only 1, it will not scale.  Ditto for memory; if memory is not reasonably maxed out
by the job, adding more memory will not help it.  Ditto for disk bandwidth.

### Sysadmin view: Are jobs using appropriate resources?

Basically this comes down to "reservation" (what the job requested, implicitly or explicitly) and
"usage" (what the job actually used).

In the ML-node domain, if a job runs on ML8 (which has four A100 cards) it implicitly is requesting
the use of GPUs.  If it does not use the GPUs it is using the resources inappropriately, because it
is hogging the CPUs that should be reserved for some job that will use the GPUs.

### Sysadmin view: Historical data about utilization

This could be for the system as a whole, broken down by user, by time of day and day of week, etc.

### Usable on a variety of systems

For a prototype we care mostly about the ML nodes, in the intermediate term about Fox, and in the
longer term perhaps the national systems as well.

But since we're talking about upgrading users to bigger systems when they're hogging systems then
getting them to move from ML to Fox (not very GPU bound) or from ML to LUMI (more GPU bound, but
AMD) is probably the first order of business.

## What is a resource

This gets us into two details:

- when is a job or user "hogging" some resource?

- there is definitely a sense here that if the system is "mostly idle" even a job that is not using
  the resources appropriately is not hogging.  Something running flat-out on one core for several
  days on a 64-core system is not actually hogging the system if the other cores are mostly idle.


# What kinds of resources are there

- CPU (number in use; load; in principle also the features used, such as AVX512)
- GPU (number in use; load; in principle also the features used or the APIs)
- CPU/main memory (real memory occupancy, averages and peaks)
- GPU memory
- PCI bandwidth, maybe
- Disk bandwidth, maybe, esp writes
- Disk usage (scratch disk)
- Other kinds of bandwidth, maybe (other interconnects than PCI)
- Interactivity / response time is a kind of resource but unclear how that fits in

Memory is very tricky, because a lot of memory is shared among threads
and paged-in read-only memory is particularly tricky because it can be
discarded and repopulated cheaply.

The amount of rw memory for a process (which could just be a thread,
and multiple threads could share these mappings):

cat /proc/683767/maps | grep ' rw' | gawk '{ l += strtonum("0x" $3) } END { print l }'

aka the simpler

gawk  '$2 = / rw/ { l += strtonum("0x" $3) } END { print l }' /proc/683767/maps

but note that /proc/N/maps is readable only by owner or root,
permissions on the file notwithstanding (444).

Re disk, we can examine disk-free but per-user usage is root-only and
probably expensive to compute.  It may be computable occasionally.
This gives rise to the notion that data are produced at different
times by independent agents.

We can figure out if something is a thread together with something
else if they have the same thread group ID?

Top without -H lumps threads' CPU time (unclear how it determines
threadedness) in with the parent process.

Htop shows threads in a different color (green on my display, while
processes are black).

## Noise / quantization effects

Jobs that run for a short time only are not interesting, really, unless there are very many of them
and they together create significant load.


## Sample-based profiling

Short of having some kind of built-in accounting, it is possible to run a sampler every little
while; the interval depends on what we're looking for, but given that we mostly care about
long-running jobs, every minute should be OK.  The sampler basically amounts to collecting data like
`top` does, then aggregating it and extracting useful information from it.

## What tools exist already?

Apparently Sigma2 uses RRD for some things, but (rumor) it's slow to receive updates and may need to
be replaced: https://en.wikipedia.org/wiki/RRDtool.  Also, this seems mostly about storage and
presentation.  The data sources are separate and must present data to RRD, which is structured
around saving data in a circular queue at regular intervals.

XDMod / Open XDMod seems like a more comprehensive tool but may be
queue-oriented (SLURM etc).  We don't have queues on the ML nodes, and
maybe not some other places.


## Data providers

nvidia-smi has a lot of options to explain what the GPUs are doing.

rocm-smi ditto, though the formats are very different.

`top -b -n 1` (or indeed without `-n`) is pretty handy.  Try `top -b -n 1 -U '!root'` for a hoot,
this is non-root processes with all their fields.

Sabry's "ML systems load calculator" does a bunch of grubbing through systems tables a la procinfo.

Sabry's script is basically
```
nvidia-smi --query-gpu=utilization.gpu,utilization.memory --format=csv,noheader,nounits | \
  awk -F , '    {l+=$1; m+=$2; b++} \
            END {print l/b \"%|\" m/b \"%\"}' "
```

## Technology

Even though this is a task for Go, it doesn't look like the future has
arrived at some of the pertinent systems, and it may be easiest to use
Python or gawk (or C++, though I would prefer not).  A go program can
be built elsewhere and copied over but ...  Python has dependency
hell, too.  Anything that needs to be installed is tricky.  Anaconda
might be desirable.


  has lots of possibilities, eg this:
```
nvidia-smi --id=0 --query-gpu=utilization.gpu,utilization.memory,memory.total,memory.free,memory.used --format=csv --loop=1
```
  Running this with a demo program I have, I see output like this, which is presumably what we want as
  "proof" that the GPU was running:
```
0 %, 0 %, 11264 MiB, 11019 MiB, 0 MiB
71 %, 21 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
100 %, 44 %, 11264 MiB, 6287 MiB, 4732 MiB
94 %, 33 %, 11264 MiB, 11019 MiB, 0 MiB
0 %, 0 %, 11264 MiB, 11019 MiB, 0 MiB
```
  We could imagine augmenting this with UID, timestamp, GPU#, maybe PID, and saving to a time-limited log of some type.



## Other notes

* Re GDPR and secrets, there are a couple of mitigations.  The user
  could opt in to the logging (through a file in the home directory,
  say), or the user could consent to logging by using the systems, if
  alerted to this.  Logs for a user could be viewable only by the user
  and root.  Also, if we say that we don't care about short-lived
  jobs then they can be culled from the log very quickly.


