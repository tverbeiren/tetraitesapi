#!/bin/bash

jobserver=127.0.0.1

DIR=$(dirname "$0")

echo
echo
echo ">> No arguments given, all results are returned"
echo

curl -d '' \
     $jobserver':8090/jobs?context=tetraites&appName=tetraitesapi&classPath=tetraitesapi.gezoTimeline&sync=true'

echo
echo
echo ">> Specify start and end for Patricia"
echo

curl -d '{
            lidano = "Patricia"
            start = 20121116
            end = 20121205
         }' \
     $jobserver':8090/jobs?context=tetraites&appName=tetraitesapi&classPath=tetraitesapi.gezoTimeline&sync=true'

echo
echo
echo ">> Specify start and end for Patricia"
echo

curl -d '{
            window = "2012.*"
         }' \
     $jobserver':8090/jobs?context=tetraites&appName=tetraitesapi&classPath=tetraitesapi.gezoTimeline&sync=true'

