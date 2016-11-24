# Introduction

Query timeline and event information from medical records using  to provide a REST API to Spark.

# Data

We use synthetic data generated to be of the same format and in line with the real data.


# Use

The fastest approach is to spin up a [Docker](https://www.docker.com/) container containing a full Spark Jobserver stack. We have done some preparations, so that should be easy...

## Setup: Word count

In order to test the setup, we will run an example provided by Spark Jobserver. Please follow the guide available [here](https://github.com/data-intuitive/spark-jobserver): <https://github.com/data-intuitive/spark-jobserver>.

The short version is this:

```bash
docker run -d -p 8090:8090 -v /tmp/api/data:/app/data tverbeiren/jobserver
```

If this step is working, you can proceed to the next one.

## Tetraites API binary release

Let's assume you've started the docker container as described in 

Start by downloading the assembly jar and store it under `/tmp/api`:

```bash
wget http://dl.bintray.com/tverbeiren/maven/info/verbeiren/tetraitesapi_2.11/0.0.2/tetraitesapi_2.11-0.0.2-assembly.jar
mv tetraitesapi_2.11-0.0.2-assembly.jar /tmp/api/
```

Now, in order to make life easy, some scripts are available. Two configuration files define the behavior of these scripts: `config/settings.sh` and `config/initialize-docker.conf`. Take a look at both to see what is going on. They are configured to work with the procedure described in this manual.

Start the Tetraites API:

```bash
scripts/initialize.sh
```

Test the available endpoints:

```
scripts/farmaTimeline.sh
scripts/gezoTimeline.sh
```

## Tetraites API source

In order to compile from source, the following procedure should be sufficient
(given all dependencies are available):

1. Clone the project from github
2. Run the script `compileAndRun.sh`

These two steps should be sufficient to be in the state ready for following
the procedure for the binary release described above.


