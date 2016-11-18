#!/bin/bash

# Some useful directories
DIR=$(dirname "$0")
BASE="$DIR/.."
CONFIG=$BASE/config

# Read the settings
source "$CONFIG/settings.sh"

# Convert app name to lowercase
APPLC=$(echo $APP | tr "[:upper:]" "[:lower:]")

URI="$jobserver:8090/jobs?context=$APPLC&appName=$APPLC&classPath=$CP.farmaTimeline&sync=true"

echo
echo
echo ">> Without arguments, all entries are returned"
echo

curl -d '' $URI     

echo
echo
echo ">> With lidano and window specified by start and end"
echo

curl -d '{
            lidano = "Karel"
            start = 20110101
            end = 20121205
         }' $URI 

echo
echo
echo ">> With lidano and window specified by regular expression"
echo

curl -d '{
            lidano = "Karel"
            window = "2011.*"
         }' $URI 

