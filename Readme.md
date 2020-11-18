Building
---
A pre-built jar is available as `assignment.jar`. However, you can easily build and run it yourself with `sbt`:

`sbt assembly` to create a "fat jar" with all dependencies, or

`sbt package` to build a jar with just this project's classes.

Running
---
The job can be executed with `spark-submit`. The parameters for the job (e.g. input file locations, desired output 
format, etc.) should be specified in a json file, which should be passed as a command-line argument. The file
`job.json` can be used for this. 

At the moment, it only supports input and output from/to local files. The granularity
of the aggregation can be set to either "Weekly" or "Yearly". Finally, a list of divisions may optionally be specified.
If you do, the job will not include sales for products from other divisions than the ones listed. Otherwise, all divisions
are included.

So, assuming `spark-submit` is on the PATH and both `assignment.jar` and `job.json` are in the current directory:

`spark-submit assignment.jar job.json`

If no changes were made to `job.json`, this will create a directory called `output`, which will contain the resulting json files.

Testing
---
Tests can be executed via `sbt test` in the project root.