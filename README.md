## DISC

An general framework for computing subgraph counting.

### Quick Start

Run following scripts to get an glimpse of DISC. It should just work.

``` 
./test_local.sh
```

### Prerequisite

You need to install Spark 2.4.3 on your cluster or your machine.

An copy of Spark 2.4.3 is included.

### Input Format

Data graph need to be stored in edge list format.

For example,

```
1 2

2 3

3 4

4 5
```

An example input file is shown in examples/email-Eu-core.txt

### Pattern Format

A pattern is of form, "A-B;B-C;C-D". DISC accepts a file that contains a batch of patterns, where the first column is the name of the pattern and the second column is the form of the pattern

For example,

```
#Pattern Name #Pattern
patternName1 A-D;A-E;B-C;B-D;B-E;C-D;C-E;
patternName2 A-B;A-C;A-D;A-E;B-C;B-D;B-E;C-D;
```

An example pattern file is shown in examples/query.txt

### Environment Parameters

There needs to be an environment configuration file with following parameters.

```
NUM_PARTITION = 4 //numbers of partitions
NUM_CORE = 4 //core of the cluster or machine
TIMEOUT = 43200 //time out
HCUBE_MEMORY_BUDGET = 500 //memory budget for HCUBE, 500MB
IS_YARN = false //is running in yarn or locally
CACHE_SIZE = 100000 //numbers of intermediate results to be cached.
```

Example environment configuration files are shown in examples/disc_local.properties and examples/disc_yarn/properties, where one is for single machine and one is for distributed distributed environment.

### Configuration

Here are some configuration that you need to specified.

``` 
DISC 0.1
Usage: DISC [options]

  -b, --queryFile <value>  path to file that store pattern graph
  -d, --data <value>       input path of data graph
  -o, --output <value>     prefix of output path
  -e, --executionMode <value>
                           [ShowPlan|Count|Result] (ShowPlan: show the query plan, Count: show sum of count, Result: execute and output counts of query)
  -u, --queryType <value>  [InducedISO|ISO|HOM] (InducedISO: Induced Isomorphism Count, ISO: Isomorphism Count, HOM: Homomorphism Count)
  -c, --orbit <value>      orbit, i.e., A
  -p, --environment <value>
                           path to environment configuration
```

An example can be found in test_local.sh

### Running

Several scripts are included for helping you running DISC locally and distributedly.

``` 
runSpark-local.sh: helper for starting spark locally
runSpark-yarn.sh: helper for starting spark distributedly
disc_local.sh: entry for running disc locally
disc_yarn.sh: entry for running disc distributedly
```

You may need to uncomment "export SPARK_HOME="./spark-2.4.3-bin-hadoop2.7" in "runSpark-yarn.sh" and "runSpark-local.sh" to avoid conflict with your own installed Spark.

An example of runnig DISC locally is shown in  test_local.sh

### Contact 

Hao Zhang [hzhang@se.cuhk.edu.hk]()



