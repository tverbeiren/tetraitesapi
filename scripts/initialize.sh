#!/bin/bash

jobserver=127.0.0.1

DIR=$(dirname "$0")

curl -X DELETE $jobserver':8090/contexts/tetraites'
curl --data-binary @target/scala-2.11/TetraitesAPI-assembly-0.0.1.jar \
     $jobserver':8090/jars/tetraitesapi'
curl -d '' \
     $jobserver':8090/contexts/tetraites?num-cpu-cores=2&memory-per-node=1g'
curl --data-binary @$DIR/initialize.conf \
     $jobserver':8090/jobs?context=tetraites&appName=tetraitesapi&classPath=tetraitesapi.initialize'

