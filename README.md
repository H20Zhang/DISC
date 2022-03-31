# DISC v.0.1

DISC is a general approach on distributed platform for subgraph counting problem, which can count any k pattern graphs with any orbits selected.



## Overview

DISC is a general approach on distributed platform for subgraph counting problem, which can count any k pattern [graphs](https://en.wikipedia.org/wiki/Graph_(discrete_mathematics)) with any orbits selected. A full description is published on VLDB 2020 with title "Distributed Subgraph Counting: A General Approach". A brief overview is as follows.

Subgraph counting or local subgraph counting, which is to count the occurrences of a user-given pattern graph *p* around every node *v* in a data graph *G*, when *v* matches to a given orbit *o* in *p*, where the orbit serves as a center to count *p*. In general, the orbit can be a node, an edge, or a set of nodes in *p*. Subgraph counting has played an important role in characterizing high-order local structures that exhibit in a large graph, and has been widely used in denser and relevant communities mining, graphlet degree distribution, discriminative features selection for link prediction, relational classification and recommendation.

DISC is general approach which can count any k pattern graphs with any orbits selected. The key idea behind is that we do local subgraph counting by [homomorphism](https://en.wikipedia.org/wiki/Graph_homomorphism) counting, which can be solved by [relational algebra](https://en.wikipedia.org/wiki/Relational_algebra) using joins, group-by and aggregation. By homomorphism counting, we do local subgraph counting by eliminating counts for those that are not subgraph [isomorphism](https://en.wikipedia.org/wiki/Graph_isomorphism) matchings from the total count for any possible matchings.

DISC can be integrated into [Spark](https://spark.apache.org) and works as a library, or it can works as a standalone program.



## Environment

[Hadoop](https://hadoop.apache.org):The Apache Hadoop software library is a framework that allows for the distributed processing of large data sets across clusters of computers using simple programming models. It is designed to scale up from single servers to thousands of machines, each offering local computation and storage. Rather than rely on hardware to deliver high-availability, the library itself is designed to detect and handle failures at the application layer, so delivering a highly-available service on top of a cluster of computers, each of which may be prone to failures.

[Spark](https://spark.apache.org):Apache Spark™ is a unified analytics engine for large-scale data processing.

DISC is tested on Hadoop 2.7.5 and Spark 2.4.5.



## Running Queries

DISC can be used as a [standalone](#Standalone) system that runs on top of Spark or as a [library](###Library) which can be integrated with Spark. Before you start up DISC, you need to configure the DISC based on your own environment (Locally or Yarn).

### Quick Start

Run following scripts to get an glimpse of DISC. It should just work.

``` 
./test_local.sh
```

### Prerequisite

You need to install Spark 2.4.5 on your cluster or your machine.

An copy of Spark 2.4.5 is included.

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

### Use with Commandline

#### Pattern Format

A pattern is of form, "A-B;B-C;C-D". DISC accepts a file that contains a batch of patterns, where the first column is the name of the pattern and the second column is the form of the pattern

For example,

```
#Pattern Name #Pattern
patternName1 A-D;A-E;B-C;B-D;B-E;C-D;C-E;
patternName2 A-B;A-C;A-D;A-E;B-C;B-D;B-E;C-D;
```

An example pattern file is shown in examples/query.txt

#### Scripts

Several scripts are included for helping you running DISC locally and distributedly.

``` 
runSpark-local.sh: helper for starting spark locally
runSpark-yarn.sh: helper for starting spark distributedly
disc_local.sh: entry for running disc locally
disc_yarn.sh: entry for running disc distributedly
```

You may need to uncomment "export SPARK_HOME="./spark-2.4.3-bin-hadoop2.7" in "runSpark-yarn.sh" and "runSpark-local.sh" to avoid conflict with your own installed Spark.

An example of runnig DISC locally is shown in  test_local.sh

### Use with Spark

To use DISC as a library in your project, you can use the compiled library (disc_2.11-0.1.jar) or you can compile and pacakge DISC by your own using [SBT](https://www.scala-sbt.org). The necessary build.sbt file has been included in the project. you may just need to modify few depedencies in build.sbt file based on your Hadoop and Spark version, and compile DISC following this simple [guide](https://alvinalexander.com/scala/sbt-how-to-compile-run-package-scala-project/).

After an compiled DISC library has been obtained, The main class you will interact with is **org.apache.spark.disc.SubgraphCounting**. 

```
  val data = pathToData
  val query = "A-B;B-C;A-C;"
  val orbit = "A"
  val queryType = QueryType.InducedISO

  //Get local subgraph counting, the return result is RDD[Array[Int]], where element at the first index is node_id, element at the second index is the count numbers.
  // When you try to run multiple query and orbit, the computation is shared implicitly.
  SubgraphCounting.rdd(pathToData, query, orbit, queryType)
  
  //Get sum of all local counts
  //It is (K * global subgraph count), where K is numbers of equvilant orbit for the given orbit.
  SubgraphCounting.count(pathToData, query, orbit, queryType)
```

## Contact 

[Hao Zhang](mailto:zhanghaowuda12@gmail.com)



