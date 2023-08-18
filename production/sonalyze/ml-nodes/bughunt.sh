#!/usr/bin/env bash

sonar_dir=$HOME/sonar
sonar_data_dir=$sonar_dir/data

SONAR_ROOT=$sonar_data_dir $sonar_dir/sonalyze jobs -u - "$@" --zombie --fmt=tag:bughunt,std,start,end,cmd
