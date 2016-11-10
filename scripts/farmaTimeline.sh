#!/bin/bash

jobserver=127.0.0.1

DIR=$(dirname "$0")

echo
echo
echo ">> Without arguments, all entries are returned"
echo

curl -d '' \
     $jobserver':8090/jobs?context=tetraites&appName=tetraitesapi&classPath=tetraitesapi.farmaTimeline&sync=true'

echo
echo
echo ">> With lidano and window specified by start and end"
echo

curl -d '{
            lidano = "Karel"
            start = 20110101
            end = 20121205
         }' \
     $jobserver':8090/jobs?context=tetraites&appName=tetraitesapi&classPath=tetraitesapi.farmaTimeline&sync=true'

echo
echo
echo ">> With lidano and window specified by regular expression"
echo

curl -d '{
            lidano = "Karel"
            window = "2011.*"
         }' \
     $jobserver':8090/jobs?context=tetraites&appName=tetraitesapi&classPath=tetraitesapi.farmaTimeline&sync=true'

