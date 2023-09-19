#!/usr/bin/env bash
#
# Run sonalyze for the `bughunt` use case and capture its output in a
# file appropriate for the current time and system.

sonar_dir=$HOME/sonar
sonar_data_dir=$sonar_dir/data

year=$(date +'%Y')
month=$(date +'%m')
day=$(date +'%d')

output_directory=${sonar_data_dir}/${year}/${month}/${day}
mkdir -p ${output_directory}

SONAR_ROOT=$sonar_data_dir $sonar_dir/sonalyze jobs -u - "$@" --zombie --fmt=csvnamed,tag:bughunt,now,std,start,end,cmd >> ${output_directory}/bughunt.csv
