#!/usr/bin/env bash

prefix="/hzhang/data"
JAR="./ADJ-assembly-0.1.1.jar"

timeout=43200
executeScript=runSpark-logo.sh
#executeScript=runSpark-logo-local.sh
querys=(triangle fourClique fiveClique house threeTriangle near5Clique fiveCliqueMinusOne)
numExecutors=28

Test() {

  method=$1
  isCommOnly=$2
  taskNum=$3
  mainClass=org.apache.spark.adj.utils.exp.ExpEntry

  # shellcheck disable=SC2068
  for i in ${input[@]}; do
    data=$i
    file="${prefix}/${data}"
    for query in ${querys[@]}; do
        echo "----------------------------------"
        echo executing $i $query $k

#        SECONDS=0
        $executeScript --num-executors $numExecutors --class $mainClass $JAR -q ${query} -t $timeout -d ${file} -c $isCommOnly -m $method -t $taskNum

#        duration=$SECONDS
#        echo "executing $executeScript  --num-executors 32  --class $mainClass $JAR -q ${query} -t $timeout -d ${file} -c $isCommOnly -m $method with $duration seconds elapsed."
    done
  done
}

#methods=(CacheHCube, Factorize, PullHCube, MergedHCube, PushHCube)
#input=(as lj webB wikiT  enwiki-2013 orkut)
#querys=(triangle fourClique fiveClique house threeTriangle near5Clique fiveCliqueMinusOne)

#input=(as)
#querys=(triangle fourClique fiveClique house threeTriangle near5Clique fiveCliqueMinusOne)
#Test PushHCube false
#Test PushHCube true

#input=(as)
#querys=(triangle fourClique fiveClique)
#Test MergedHCube false
#Test MergedHCube true

#input=(webB)
#querys=(threeTriangle near5Clique)
#Test CacheHCube false 196

input=(lj)
querys=(near5Clique)
Test CacheHCube false 196

#input=(lj)
#querys=(near5Clique)
#Test PushHCube false 1536


