#!/usr/bin/env bash


#
#Test() {
#
#  method=$1
#  isCommOnly=$2
#  taskNum=$3
#  mainClass=org.apache.spark.adj.adj.utils.exp.ExpEntry
#
#  # shellcheck disable=SC2068
#  for i in ${input[@]}; do
#    data=$i
#    file="${prefix}/${data}"
#    for query in ${querys[@]}; do
#      echo "----------------------------------"
#      echo executing $i $query $k
#
#      #        SECONDS=0
#      $executeScript --num-executors $numExecutors --class $mainClass $JAR -q ${query} -t $timeout -d ${file} -c $isCommOnly -m $method -n $taskNum -s $numSamples
#
#      #        duration=$SECONDS
#      #        echo "executing $executeScript  --num-executors 32  --class $mainClass $JAR -q ${query} -t $timeout -d ${file} -c $isCommOnly -m $method with $duration seconds elapsed."
#    done
#  done
#}

mainClass=org.apache.spark.dsce.util.testing.ExpEntry
prefix="/hzhang/benu/data"
JAR="./DISC-assembly-0.1.jar"
timeout=43200
executeScript=runSpark-logo.sh
cacheSize=1000000
numExecutors=28
#executeScript=runSpark-logo-local.sh

TestScalability() {
  ks=(1 2 4 8 16 28)
  method=$1
  isCommOnly=$2
  taskNum=$3
  mainClass=org.apache.spark.adj.adj.utils.exp.ExpEntry
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



Execute() {
  inputs=$1
  patterns=$2
  executionMode=$3
  queryType=$4
  core=$5
  # shellcheck disable=SC2068
  for j in ${patterns[@]}; do
    for i in ${inputs[@]}; do
    input=$i
    inputFile="${prefix}/${input}"
    query=$j
    echo "$executeScript --class $mainClass $JAR -d ${inputFile} -q $query  -e $executionMode -u $queryType -c $core -s $cacheSize  -t $timeout"
    $executeScript --class $mainClass $JAR -d ${inputFile} -q $query  -e $executionMode -u $queryType -c $core -s $cacheSize  -t $timeout

    done
  done
}

DEBUGTask() {
  #inputs=(wv ep soc-Slashdot wiki-Talk ego-Twitter wb as lj ok uk soc-lj)
  #patterns=(house threeTriangle solarSquare near5Clique)
  executionMode="Count"
  queryType="Partial"
  core="A;B"
  inputs=(wb)
  patterns=(square)

  Execute $inputs $patterns $executionMode $queryType $core
}

5NodePatternTask_Node() {
#  inputs=(wv ep soc-Slashdot wiki-Talk ego-Twitter wb as lj soc-lj ok uk)
#  patterns=(house threeTriangle solarSquare near5Clique)
  executionMode="Count"
  queryType="NonInduce"
  core="A"
#  inputs=(wv ep soc-Slashdot ego-Twitter wb as lj soc-lj ok uk)
#  patterns=(house threeTriangle near5Clique solarSquare)
  inputs=(as)
  patterns=(solarSquare)

  Execute $inputs $patterns $executionMode $queryType $core
}

5NodePatternTask_Edge() {
  #inputs=(wv ep soc-Slashdot wiki-Talk ego-Twitter wb as lj soc-lj ok uk)
  #patterns=(house threeTriangle solarSquare near5Clique)
  executionMode="Count"
  queryType="NonInduce"
  core="A;B"
  inputs=(as)
  patterns=(house)

  Execute $inputs $patterns $executionMode $queryType $core
}

6NodePatternTask() {
#  inputs=(soc-Slashdot wiki-Talk ego-Twitter)
#  patterns=(quadtriangle triangleCore twinCSquare twinClique4 starofDavidPlus)

  executionMode="Count"
  queryType="NonInduce"
  core="A"
  inputs=(wv)
  patterns=(quadtriangle)

  Execute $inputs $patterns $executionMode $queryType $core
}




echo "---------------DISC---------------"

5NodePatternTask_Node
#5NodePatternTask_Edge
#6NodePatternTask
#DEBUGTask


