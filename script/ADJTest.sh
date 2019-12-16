#!/usr/bin/env bash

prefix="/hzhang/benu/data"
JAR="./ADJ-assembly-0.1.1.jar"

#timeout=21600
timeout=21600
executeScript=runSpark-logo.sh
#executeScript=runSpark-logo-local.sh
querys=(triangle fourClique fiveClique house threeTriangle near5Clique fiveCliqueMinusOne)
numExecutors=28
numSamples=100000

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
      $executeScript --num-executors $numExecutors --class $mainClass $JAR -q ${query} -t $timeout -d ${file} -c $isCommOnly -m $method -n $taskNum -s $numSamples

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
        $executeScript --num-executors $k --class $mainClass $JAR -q ${query} -t $timeout -d ${file} -c $isCommOnly -m $method -n $taskNum -s $numSamples
      done
    done
  done
}

ConvertWebGraph() {
  input=$1
  output=$2
  mainClass=org.apache.spark.adj.adj.utils.misc.WebGraphConverter
  $executeScript --num-executors $numExecutors --class $mainClass $JAR $input $output
}

input=(wb)
querys=(square)
Test CacheHCube Count 196

#methods=(CacheHCube, Factorize, PullHCube, MergedHCube, PushHCube, ADJ, SPARKSQL)
#input=(as lj webB wikiT en orkut)
#modes=(ShowPlan CommOnly Count)
#querys=(triangle fourClique fiveClique house threeTriangle near5Clique fiveCliqueMinusOne triangleEdge square chordalSquare threePath)

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

#input=(as)
#querys=(house threeTriangle near5Clique)
#Test ADJ false 196
#
#input=(as)
#querys=(house threeTriangle near5Clique)
#Test CacheHCube false 196
#
#input=(as)
#querys=(house threeTriangle near5Clique)
#Test PushHCube false 196

#input=(as orkut)
#querys=(house threeTriangle near5Clique)
#Test MergedHCube ShowPlan 196
#
#input=(as)
#querys=(house threeTriangle near5Clique)
#Test ADJ ShowPlan 196
#
#input=(as)
#querys=(house threeTriangle near5Clique)
#Test MergedHCube CommOnly 196

#input=(as)
#querys=(near5Clique)
#Test ADJ ShowPlan 196
#
#input=(as)
#querys=(near5Clique)
#Test ADJ CommOnly 196
#
#input=(as)
#querys=(near5Clique)
#Test ADJ Count 196
#
#input=(lj)
#querys=(triangle)
#TestScalability MergedHCube Count 196
#
#input=(lj)
#querys=(fourClique)
#TestScalability MergedHCube Count 196

#input=(lj)
#querys=(fiveClique)
#TestScalability MergedHCube Count 196

#input=(lj)
#querys=(house)
#TestScalability ADJ Count 196

#ConvertWebGraph /hzhang/data/webGraph/enwiki-2013-nat /hzhang/data/en

#input=(en)
#querys=(triangle)
#Test MergedHCube Count 196

#input=(as lj webB wikiT en orkut)
#querys=(chordalSquare triangleEdge threePath)
#Test ADJ Count 196
#
#input=(as lj webB wikiT en orkut)
#querys=(square)
#Test CacheHCube Count 196

#input=(en)
#querys=(triangle fourClique fiveClique)
#Test MergedHCube Count 196
#
#input=(en)
#querys=(triangle fourClique fiveClique)
#Test PullHCube Count 196
#
#input=(en)
#querys=(triangle fourClique fiveClique)
#Test PushHCube Count 196
#
#input=(en)
#querys=(triangle fourClique fiveClique)
#Test CacheHCube Count 196
#
#
#input=(en)
#querys=(triangle fourClique fiveClique)
#Test MergedHCube CommOnly 196
#
#input=(en)
#querys=(triangle fourClique fiveClique)
#Test PullHCube CommOnly 196
#
#input=(en)
#querys=(triangle fourClique fiveClique)
#Test PullHCube CommOnly 196

#timeout=43200
#input=(as)
#querys=(near5Clique threeTriangle)
#Test MergedHCube Count 196

#input=(as)
#querys=(near5Clique)
#Test PushHCube CommOnly 196
#
#input=(lj)
#querys=(triangle)
#TestScalability MergedHCube Count 196
#
#input=(lj)
#querys=(house)
#TestScalability ADJ Count 196
#
#input=(lj)
#querys=(triangle)
#TestScalability MergedHCube Count 196
#
#input=(lj)
#querys=(house)
#TestScalability ADJ Count 196

#timeout=10080
#input=(lj)
#querys=(fiveClique)
#TestScalability MergedHCube Count 196

#for (( i = 0; i < 5; i++ )); do
#
#echo $i
#done

#
#
# shellcheck disable=SC1073
#for (( i = 0; i < 5; i++ )); do
#numSamples=100
#input=(lj)
#querys=(house)
#Test ADJ ShowPlan 196
#
#numSamples=1000
#input=(lj)
#querys=(house)
#Test ADJ ShowPlan 196
#
#numSamples=10000
#input=(lj)
#querys=(house)
#Test ADJ ShowPlan 196
#
#numSamples=100000
#input=(lj)
#querys=(house)
#Test ADJ ShowPlan 196
#
#numSamples=1000000
#input=(lj)
#querys=(house)
#Test ADJ ShowPlan 196
#
#numSamples=10000000
#input=(lj)
#querys=(house)
#Test ADJ ShowPlan 196
#done
#
#for (( i = 0; i < 5; i++ )); do
#numSamples=100
#input=(lj)
#querys=(threeTriangle)
#Test ADJ ShowPlan 196
#
#numSamples=1000
#input=(lj)
#querys=(threeTriangle)
#Test ADJ ShowPlan 196
#
#numSamples=10000
#input=(lj)
#querys=(threeTriangle)
#Test ADJ ShowPlan 196
#
#numSamples=100000
#input=(lj)
#querys=(threeTriangle)
#Test ADJ ShowPlan 196
#
#numSamples=1000000
#input=(lj)
#querys=(threeTriangle)
#Test ADJ ShowPlan 196
#
#numSamples=10000000
#input=(lj)
#querys=(threeTriangle)
#Test ADJ ShowPlan 196
#done

#input=(lj)
#querys=(house threeTriangle near5Clique)
#
#for ((i = 0; i < 5; i++)); do
#  echo "iteration-${i}"
#  numSamples=200
#  echo "samples size -${numSamples}"
#  Test ADJ ShowPlan 196
#
#  numSamples=500
#  echo "samples size -${numSamples}"
#  Test ADJ ShowPlan 196
#
#  numSamples=1000
#  echo "samples size -${numSamples}"
#  Test ADJ ShowPlan 196
#
#  numSamples=10000
#  echo "samples size -${numSamples}"
#  Test ADJ ShowPlan 196
#
#  numSamples=100000
#  echo "samples size -${numSamples}"
#  Test ADJ ShowPlan 196
#
#  numSamples=1000000
#  echo "samples size -${numSamples}"
#  Test ADJ ShowPlan 196
#
#  numSamples=10000000
#  echo "samples size -${numSamples}"
#  Test ADJ ShowPlan 196
#done


#sampleTest() {
#  numSamples=$1
#  for i in {1..5} ; do
#  echo "samples size -${numSamples}"
#  Test ADJ ShowPlan 196
#  done
#}
#
#input=(lj)
#querys=(house)
#sampleTest 200
#sampleTest 500
#sampleTest 1000
#sampleTest 10000
#sampleTest 100000
#sampleTest 1000000
#sampleTest 10000000
#
#input=(lj)
#querys=(threeTriangle)
#sampleTest 200
#sampleTest 500
#sampleTest 1000
#sampleTest 10000
#sampleTest 100000
#sampleTest 1000000
#sampleTest 10000000
#
#input=(lj)
#querys=(near5Clique)
#sampleTest 200
#sampleTest 500
#sampleTest 1000
#sampleTest 10000
#sampleTest 100000
#sampleTest 1000000
#sampleTest 10000000

#input=(lj)
#querys=(house)
#for i in {1..5} ; do
#numSamples=200
#echo "samples size -${numSamples}"
#Test ADJ ShowPlan 196
#done
#
#for i in {1..5} ; do
#numSamples=500
#echo "samples size -${numSamples}"
#Test ADJ ShowPlan 196
#done
#
#for i in {1..5} ; do
#numSamples=1000
#echo "samples size -${numSamples}"
#Test ADJ ShowPlan 196
#done
#
#for i in {1..5} ; do
#input=(lj)
#querys=(house)
#numSamples=10000
#echo "samples size -${numSamples}"
#Test ADJ ShowPlan 196
#done
#
#for i in {1..5} ; do
#input=(lj)
#querys=(house)
#numSamples=100000
#echo "samples size -${numSamples}"
#Test ADJ ShowPlan 196
#done
#
#for i in {1..5} ; do
#input=(lj)
#querys=(house)
#numSamples=1000000
#echo "samples size -${numSamples}"
#Test ADJ ShowPlan 196
#done
#
#for i in {1..5} ; do
#input=(lj)
#querys=(house)
#numSamples=10000000
#echo "samples size -${numSamples}"
#Test ADJ ShowPlan 196
#done

#input=(lj)
#querys=(near5Clique)
#Test ADJ ShowPlan 196

#input=(lj)
#querys=(threeTriangle near5Clique)
#TestScalability ADJ CommOnly 196

#timeout=20000
#input=(lj)
#querys=(fiveClique)
#TestScalability MergedHCube Count 196


#input=(as)
#querys=(house)
#Test MergedHCube ShowPlan 196

#input=(as)
#querys=(threeTriangle near5Clique)
#numSamples=100000
#echo "samples size -${numSamples}"
#Test ADJ ShowPlan 196

#input=(en)
#querys=(triangle)
#Test SPARKSQL Count 196