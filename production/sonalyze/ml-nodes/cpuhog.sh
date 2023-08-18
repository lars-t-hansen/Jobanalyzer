#!/usr/bin/env bash

sonar_dir=$HOME/sonar
sonar_data_dir=$sonar_dir/data

# Jobs that have used "a lot" of CPU and have run for at least 10 minutes but have not touched the GPU.
# Reports go to stdout.  It runs on the data for the last 24h.  It should run about once every 12h.
# It's possible that the sensible thing to do here is to send CSV to a file and send formatted output
# to stdout for email consumption.
#
# What's "a lot" of CPU?  We can define this as a peak of at least 20 cores and an average of at least 10.
# But of course this doesn't quite capture it, because there's some impact on how long things ran
# and so on, there's a "window of interest".  If the job powers up, runs a lot of stuff for some significant time, and then just sits
# there for a long time, the average will eventually drop below the trigger.  We'll capture it during the early
# phase maybe, but it comes down to how often we run this analysis.

#SONAR_ROOT=$sonar_data_dir $sonar_dir/sonalyze jobs -u - --no-gpu --min-cpu-peak=1000 --min-runtime=10m --fmt=csv,header,tag:cpuhog,job,user,duration,cpu-avg,cpu-peak,gpu-avg,gpu-peak,host,cmd

# The "max-rgpu" queries work around a temporary bug (17 August 2023) in the input data, they can be removed soon.
SONAR_ROOT=$sonar_data_dir $sonar_dir/sonalyze jobs --config-file=$sonar_dir/ml-nodes.json -u - --no-gpu --min-rcpu-peak=10 --min-runtime=10m --max-rgpu-peak=10000 --max-rgpu-avg=10000 --fmt=tag:cpuhog,job,user,duration,cpu-avg,cpu-peak,gpu-avg,gpu-peak,rcpu-avg,rcpu-peak,rmem-avg,rmem-peak,host,start,end,cmd
