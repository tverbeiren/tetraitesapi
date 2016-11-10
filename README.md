# Introduction

Query timeline and event information from medical records.

# Data

We use synthetic data.

## Endpoints

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

# Disclaimer

Please note:

- No validation of input is done (yet)
- Spark 2.0.1 is used, which means Spark-Jobserver has to be tuned
