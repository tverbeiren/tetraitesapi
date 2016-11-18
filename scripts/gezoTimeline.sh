#!/bin/bash

# Some useful directories
DIR=$(dirname "$0")
BASE="$DIR/.."
CONFIG=$BASE/config

# Read the settings
source "$CONFIG/settings.sh"

# Convert app name to lowercase
APPLC=$(echo $APP | tr "[:upper:]" "[:lower:]")

URI="$jobserver:8090/jobs?context=$APPLC&appName=$APPLC&classPath=$CP.gezoTimeline&sync=true"

echo
echo
echo ">> No arguments given, all results are returned"
echo

curl -d '' $URI

echo
echo
echo ">> Specify start and end for Patricia"
echo

curl -d '{
            lidano = "Patricia"
            start = 20121116
            end = 20121205
         }' $URI

echo
echo
echo ">> Specify start and end for Patricia"
echo

curl -d '{
            window = "2012.*"
         }' $URI

