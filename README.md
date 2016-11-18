# Introduction

Query timeline and event information from medical records using [Spark Jobserver](https://github.com/spark-jobserver/spark-jobserver) to provide a REST API to Spark.

# Data

We use synthetic data generated to be of the same format and in line with the real data.


# Use

The easist approach is to spin up a [Docker](https://www.docker.com/) container containing a full Spark Jobserver stack. We have done some preparations, so that should be easy...

## Setup: Word count

In order to test the setup, we will run an example provided by Spark Jobserver. Please follow the guide available [here](https://github.com/data-intuitive/spark-jobserver): <https://github.com/data-intuitive/spark-jobserver>.

The short version is this:

```bash
docker run -d -p 8090:8090 -v /tmp/api/data:/app/data tverbeiren/jobserver
```

If this step is working, you can proceed to the next one.

## Tetraites API

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

- - -

In what follows, we provide more in-depth documentation about the endpoints.

# Endpoints

### `initialize`

Initialize the application by creating a context and pushing the `jar` file:

```bash
curl -X DELETE $jobserver':8090/contexts/tetraites'
curl --data-binary @target/scala-2.11/TetraitesAPI-assembly-0.0.1.jar \
     $jobserver':8090/jars/tetraitesapi'
curl -d '' \
     $jobserver':8090/contexts/tetraites?num-cpu-cores=2&memory-per-node=1g'
curl --data-binary @$DIR/initialize.conf \
     $jobserver':8090/jobs?context=tetraites&appName=tetraitesapi&classPath=tetraitesapi.initialize'
```

Input parameters in `initialize.conf`:

- __gezoDb__: path to gezo file (tab-separated)
- __farmaDb__: path to farma file (tab-separated)
- __atcDict__: path to files with ATC codes (space-separated, currently only the first 2 are used)

Example `initialize.conf`:

```json
{
  gezoDb  = "<path>/gezo.txt"
  farmaDb = "<path>/farma.txt"
  atcDict = "<path>/ATCDPP.CSV <path>/atcCodes.txt"
}
```

Please note that we add 2 dictionary files as a simple list with a space in-between.

### `gezoTimeline` and `farmaTimeline`

Query the timeline for one of more lidano's for a certain window of time.

Input parameters:

- __lidano__: A regular expression for the lidano key (default: `.*`)
- __start__: the start date of a window of interest (default: `19000101`)
- __end__: The end date of a window of interest (default: `25000101`)
- __window__: Specify a window using regular expressions on the string dates (default: `.*`)

Example call:

```
curl -d '{
            lidano = "Patricia"
            start = 20121116
            end = 20121205
         }' \
     $jobserver':8090/jobs?context=tetraites&appName=tetraitesapi&classPath=tetraitesapi.gezoTimeline&sync=true'
```


# Disclaimer

Please note:

- No validation of input is done (yet)
- Spark 2.0.1 is used, which means Spark-Jobserver has to be tuned
