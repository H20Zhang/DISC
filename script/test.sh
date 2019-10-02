#!/usr/bin/env bash

prefix="/hzhang/data"
JAR="./ADJ-assembly-0.1.1.jar"

#timeout=21600
timeout=21600
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

TestScalability() {
  ks=(1 2 4 8 16 28)
  method=$1
  isCommOnly=$2
  taskNum=$3
  mainClass=org.apache.spark.adj.utils.exp.ExpEntry
#  executeScript=runSpark-logo-vcore.sh

  # shellcheck disable=SC2068
  for k in ${ks[@]}; do
    for i in ${input[@]}; do
      data=$i
      file="${prefix}/${data}"
      for query in ${querys[@]}; do
        echo "----------------------------------"
        echo executing $i $query $k
        $executeScript --num-executors $k --class $mainClass $JAR -q ${query} -t $timeout -d ${file} -c $isCommOnly -m $method -t $taskNum
      done
    done
  done
}

#methods=(CacheHCube, Factorize, PullHCube, MergedHCube, PushHCube, ADJ, SPARKSQL)
#input=(as lj webB wikiT orkut)
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

#input=(orkut)
#querys=(triangle)
#Test MergedHCube false 196

#input=(lj)
#querys=(near5Clique)
#Test PushHCube false 1536

#input=(orkut)
#querys=(fiveClique)
#Test MergedHCube false 196

#input=(orkut)
#querys=(fiveClique)
#Test MergedHCube false 196
#
#input=(wikiT)
#querys=(triangle)
#Test SPARKSQL false 196

#input=(lj)
#querys=(threeTriangle)
#Test ADJ false 196
#
#input=(orkut)
#querys=(near5Clique threeTriangle)
#Test CacheHCube false 196

#input=(lj)
#querys=(fiveClique)
#Test PushHCube false 196

#input=(lj)
#querys=(near5Clique)
#TestScalability ADJ false 196
#
#input=(lj)
#querys=(threeTriangle)
#TestScalability ADJ false 196



#For Comparing Push-Pull-Merge
#input=(as lj webB wikiT orkut)
#querys=(fourClique)
#Test PushHCube true 196
#
#input=(as lj webB wikiT orkut)
#querys=(fourClique)
#Test PullHCube true 196
#
#input=(as lj webB wikiT orkut)
#querys=(fourClique)
#Test MergedHCube true 196
#
#input=(as lj webB wikiT orkut)
#querys=(fourClique)
#Test PushHCube false 196
#
#input=(as lj webB wikiT orkut)
#querys=(fourClique)
#Test PullHCube false 196
#
#input=(as lj webB wikiT orkut)
#querys=(fourClique)
#Test MergedHCube false 196


#For Effectiveness of Optimizer
#input=(webB as)
#querys=(house threeTriangle near5Clique)
#Test ADJ false 196
#
#input=(webB as)
#querys=(house threeTriangle near5Clique)
#Test CacheHCube false 196

#input=(lj orkut)
#querys=(house threeTriangle near5Clique)
#Test MergedHCube true 196
#
#input=(lj)
#querys=(near5Clique)
#Test MergedHCube false 196

input=(lj)
querys=(house threeTriangle near5Clique)
Test MergedHCube true 196