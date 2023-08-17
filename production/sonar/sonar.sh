#!/usr/bin/env bash

set -euf -o pipefail

SONAR_ROOT=/itf-fi-ml/home/larstha/sonar
sonar_directory=$SONAR_ROOT/data

year=$(date +'%Y')
month=$(date +'%m')
day=$(date +'%d')

output_directory=${sonar_directory}/${year}/${month}/${day}

mkdir -p ${output_directory}

# --batchless is for systems without a job queue

$SONAR_ROOT/sonar ps --exclude-system-jobs --exclude-commands=bash,sshd,zsh,tmux,systemd --min-cpu-time=60 --batchless --rollup >> ${output_directory}/${HOSTNAME}.csv
