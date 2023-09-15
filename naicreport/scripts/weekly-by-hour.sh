#!/bin/bash
#
# This is meant to be run in a directory that has everything, symlinked if necessary

mkdir -p ./output
./naicreport ml-webload -tag weekly -sonalyze ./sonalyze -config-file ./ml-nodes.json -output-path ./output -data-path ./data -from 7d
