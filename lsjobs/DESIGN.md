(Some notes on lsjobs)


Probably filter jobs by user name (eg root) or by name (crond, sh)


Possibly filter jobs with only a single sample.


Probably show cumulative CPU resources, if we had them.  (This is more
important for the ML nodes than for the supercomputers, and it's
possible we should add fields for it.)  We can estimate them as the
observed CPU usage at sample points, but this is strictly second-class
to just having the data.  That said, we'll need to make that
estimation for memory, and for GPU.



Once we have a vector of LogEntries per job, we want to create
aggregate data for the job.  Let NREC be the number of records for the
job.

The average CPU/GPU/MEM consumption is the sum of cpu percentage
across the records, divided by NREC.

The peak is the maximum across the records.


lsjobs --hogs

Let's look for jobs with "inappropriate" resource use.  These use
"lots" of CPU and/or RAM and "little" GPU over their lifetime.

Or, they use more than 1/4 of the CPUs or 1/2 of the RAM.  (Maybe.)


lsjobs --deadweight

Zombies, forever-running `top` processes...

