# Some sketches

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
- memory (real memory occupancy, averages and peaks)
- PCI bandwidth, maybe
- Disk bandwidth, maybe, esp writes
- Other kinds of bandwidth, maybe (other interconnects than PCI)
- Interactivity / response time is a kind of resource but unclear how that fits in


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

XDMod / Open XDMod seems like a more comprehensive tool but may be queue-oriented (SLURM etc)

## Data providers

nvidia-smi has a lot of options to explain what the GPUs are doing.

rocm-smi ditto, though the formats are very different.

`top -b -n 1` (or indeed without `-n`) is pretty handy.  Try `top -b -n 1 -U '!root'` for a hoot,
this is non-root processes with all their fields.
