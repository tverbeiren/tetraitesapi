#!/bin/bash

# Some useful directories
DIR=$(dirname "$0")
BASE="$DIR/.."
CONFIG=$BASE/config

# Read the settings
source "$CONFIG/settings.sh"

# Convert app name to lowercase
APPLC=$(echo $APP | tr "[:upper:]" "[:lower:]")

echo
echo ">> Sbt Assembly"
cd $BASE
sbt assembly

echo

echo ">> Copy assembly to target dir"

cp target/scala-2.11/tetraitesapi-assembly-$APP_VERSION.jar $APP_PATH/$APPLC_2.11-$APP_VERSION-assembly.jar
