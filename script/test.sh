#!/usr/bin/env bash


#DISC options
mainClass=org.apache.spark.dsce.testing.ExpEntry
prefix="/hzhang/benu/data"
JAR="./DISC-assembly-0.1.jar"
timeout=86400
executeScript=runSpark-logo.sh
cacheSize=1000000
numExecutors=28
#executeScript=runSpark-logo-local.sh



TestScalability() {
  ks=(1 2 4 8 12 16)
  inputs=$1
  patterns=$2
  executionMode=$3
  queryType=$4
  core=$5
  platform=$6
  # shellcheck disable=SC2068
  for j in ${patterns[@]}; do
    for i in ${inputs[@]}; do
      for k in ${ks[@]}; do
    input=$i
    inputFile="${prefix}/${input}"
    query=$j
    echo "$executeScript --num-executors $k --class $mainClass $JAR -d ${inputFile} -q $query  -e $executionMode -u $queryType -c $core -s $cacheSize  -t $timeout -p $platform"
    $executeScript --num-executors $k --class $mainClass $JAR -d ${inputFile} -q $query  -e $executionMode -u $queryType -c $core -s $cacheSize  -t $timeout -p $platform
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
  platform=$6
  # shellcheck disable=SC2068
  for j in ${patterns[@]}; do
    for i in ${inputs[@]}; do
    input=$i
    inputFile="${prefix}/${input}"
    query=$j
    echo "$executeScript --class $mainClass $JAR -d ${inputFile} -q $query  -e $executionMode -u $queryType -c $core -s $cacheSize  -t $timeout -p $platform"
    $executeScript --class $mainClass $JAR -d ${inputFile} -q $query  -e $executionMode -u $queryType -c $core -s $cacheSize  -t $timeout -p $platform

    done
  done
}

DEBUGTask() {
  #inputs=(wv ep soc-Slashdot wiki-Talk ego-Twitter wb as lj ok uk soc-lj)
  #patterns=(house threeTriangle solarSquare near5Clique)
  platform="Dist"
  executionMode="Count"
  queryType="Partial"
  core="A;B"
  inputs=(wb)
  patterns=(square)

  Execute $inputs $patterns $executionMode $queryType $core $platform
}

ExtraTask() {
  #inputs=(wv ep soc-Slashdot wiki-Talk ego-Twitter wb as lj ok uk soc-lj)
  #patterns=(house threeTriangle solarSquare near5Clique)
#  platform="Dist"
#  executionMode="Count"
#  queryType="Partial"
#  core="A;B"
#  inputs=(wb)
#  patterns=(square)
  executeScript=runSpark-logo-local.sh
  mainClass=org.apache.spark.dsce.testing.ExtraExpEntry
  $executeScript --class $mainClass $JAR 6
  $executeScript --class $mainClass $JAR 7
  $executeScript --class $mainClass $JAR 8
}

TriangleTask() {
#  inputs=(wv ep soc-Slashdot wiki-Talk ego-Twitter wb as lj soc-lj ok uk)
#  inputs=(ego-Twitter wb as soc-lj ok uk)
#  patterns=(house threeTriangle solarSquare near5Clique)

  platform="Dist"
  inputs=(wb as soc-lj ok uk)
#  inputs=(wb)
  patterns=(triangle)
  executionMode="Count"
  queryType="Partial"
  core="A"

  Execute $inputs $patterns $executionMode $queryType $core $platform
}

5NodePatternTask_Node() {
#  inputs=(wv ep soc-Slashdot wiki-Talk ego-Twitter wb as lj soc-lj ok uk)
#  inputs=(ego-Twitter wb as soc-lj ok uk)
#  patterns=(house threeTriangle solarSquare near5Clique)

  platform="Dist"
  executionMode="Count"
  queryType="Partial"
  core="A"

  inputs=(wb)
  patterns=(house)

#  inputs=(wb as soc-lj ok uk)
#  patterns=(house threeTriangle near5Clique)

  Execute $inputs $patterns $executionMode $queryType $core $platform
}

5NodePatternTask_Edge() {
  #inputs=(wv ep soc-Slashdot wiki-Talk ego-Twitter wb as lj soc-lj ok uk)
  platform="Dist"
  inputs=(ego-Twitter wb as lj soc-lj ok uk)
  patterns=(house threeTriangle near5Clique)
  executionMode="Count"
  queryType="NonInduce"
  core="A;B"
  Execute $inputs $patterns $executionMode $queryType $core $platform

  patterns=(solarSquare)
  inputs=(ego-Twitter wb as lj soc-lj)
  Execute $inputs $patterns $executionMode $queryType $core $platform
}

6NodePatternTask() {
#  inputs=(soc-Slashdot wiki-Talk ego-Twitter)
#  patterns=(quadTriangle triangleCore twinCSquare twinClique4 starofDavidPlus)
  platform="Dist"
  patterns=(quadTriangle triangleCore twinCSquare twinClique4)
  executionMode="Count"
  queryType="NonInduce"
  core="A"
  inputs=(ego-Twitter wb as)

  Execute $inputs $patterns $executionMode $queryType $core $platform
}

4ProfileTask() {
  #inputs=(wv ep soc-Slashdot wiki-Talk ego-Twitter wb as lj soc-lj ok uk)
#  inputs=(wb as lj soc-lj ok)
#  inputs=(ac tp rc fb)
  platform="Dist"
  inputs=(ac)
#  inputs=(ego-Twitter wb as soc-lj)

#  patterns=(4profile)
  patterns=(fiveCycle)
  executionMode="Count"
#  queryType="Induce"
  queryType="Partial"
  core="A"
  executeScript=runSpark-logo-local_1.sh
  Execute $inputs $patterns $executionMode $queryType $core $platform
  executeScript=runSpark-logo.sh
}

5ProfileTask() {
#  inputs=(ac tp rc fb)
#  inputs=(ac)

  JAR="./DISC-assembly-0.1_no_share.jar"
  JAR="./DISC-assembly-0.1_no_merge.jar"
  platform="Parallel"
  inputs=(ac tp rc fb)
  patterns=(5profile)
  executionMode="Count"
  queryType="Induce"
  core="A"
#  executeScript=runSpark-logo-local_1.sh
  executeScript=runSpark-logo-local.sh
  Execute $inputs $patterns $executionMode $queryType $core $platform
  executeScript=runSpark-logo.sh
}


5EdgeTask() {
  #inputs=(wv ep soc-Slashdot wiki-Talk ego-Twitter wb as lj soc-lj ok uk)
  JAR="./DISC-assembly-0.1_no_share.jar"
  JAR="./DISC-assembly-0.1_no_merge.jar"
  platform="Parallel"
  inputs=(ac tp rc fb)
  patterns=(5edge)
  cacheSize=1000000
  executionMode="Count"
  queryType="Induce"
  core="A;B"
  executeScript=runSpark-logo-local.sh
  Execute $inputs $patterns $executionMode $queryType $core $platform
  executeScript=runSpark-logo.sh
}

StatisticTask() {
  inputs=(ac tp rc fb)
  executionMode="Count"
  core="A"
  platform="Parallel"

#  queryType="NonInduce"
#  patterns=(edge wedge triangle threePath threeStar triangleEdge chordalSquare fourClique square)
#  Execute $inputs $patterns $executionMode $queryType $core


#  queryType="Partial"
#  patterns=(wedge triangle fourClique)
#  Execute $inputs $patterns $executionMode $queryType $core

#  queryType="Partial"
#  patterns=(edge triangle wedge threePath square chordalSquare fourClique fiveClique)
#  Execute $inputs $patterns $executionMode $queryType $core $platform

  executeScript=runSpark-logo-local.sh
  queryType="Induce"
  patterns=(threePath threeStar square triangleEdge chordalSquare fourClique)
  Execute $inputs $patterns $executionMode $queryType $core $platform
  executeScript=runSpark-logo.sh
}

ScalabilityTask() {
  inputs=(as)
  platform="Dist"
  executionMode="Count"
  core="A"

  queryType="Induce"
  patterns=(house threeTriangle solarSquare near5Clique g1 g2 g3 g4 triangle)

  TestScalability $inputs $patterns $executionMode $queryType $core $platform
}

NodePairTask() {
#  inputs=(ego-Twitter wb as soc-lj ok uk)
  inputs=(uk)
#  inputs=(ego-Twitter)
  splitNum=300
  mainClass=org.apache.spark.dsce.testing.tool.NodePairComputer
  # shellcheck disable=SC2068

    for i in ${inputs[@]}; do
      input=$i
      inputFile="${prefix}/${input}"

      echo "$executeScript --class $mainClass $JAR ${inputFile} ${splitNum}"
      $executeScript --class $mainClass $JAR ${inputFile} ${splitNum}
    done
}

PlanGenerationTask() {
  #inputs=(wv ep soc-Slashdot wiki-Talk ego-Twitter wb as lj soc-lj ok uk)

#  inputs=(ac)
#  inputs=(ego-Twitter wb as soc-lj)

  platform="Dist"

#  inputs=(ac)
#  patterns=(showplan)
#  core="A"
#  cacheSize=100000
#  executionMode="ShowPlan"
#  queryType="Induce"
#  Execute $inputs $patterns $executionMode $queryType $core $platform


  inputs=(ac)
  patterns=(4profile)
  core="A"
  cacheSize=100000
  executionMode="ShowPlan"
  queryType="Induce"
  Execute $inputs $patterns $executionMode $queryType $core $platform

  inputs=(ac)
  patterns=(5profile)
  core="A"
  cacheSize=100000
  executionMode="ShowPlan"
  queryType="Induce"
  Execute $inputs $patterns $executionMode $queryType $core $platform


  inputs=(ac)
  patterns=(5edge)
  core="A;B"
  cacheSize=100000
  executionMode="ShowPlan"
  queryType="Induce"
  Execute $inputs $patterns $executionMode $queryType $core $platform

#  inputs=(ac)
#  patterns=(triangle)
#  core="A"
#  cacheSize=100000
#  executionMode="ShowPlan"
#  queryType="Induce"
#  Execute $inputs $patterns $executionMode $queryType $core $platform

  inputs=(ac)
  patterns=(house threeTriangle solarSquare near5Clique)
  core="A"
  cacheSize=100000
  executionMode="ShowPlan"
  queryType="NonInduce"
  Execute $inputs $patterns $executionMode $queryType $core $platform
#
  inputs=(ac)
  patterns=(house threeTriangle solarSquare near5Clique)
  core="A;B"
  cacheSize=100000
  executionMode="ShowPlan"
  queryType="NonInduce"
  Execute $inputs $patterns $executionMode $queryType $core $platform
#
  inputs=(ac)
  patterns=(quadTriangle triangleCore twinCSquare twinClique4)
  core="A"
  cacheSize=100000
  executionMode="ShowPlan"
  queryType="NonInduce"
  Execute $inputs $patterns $executionMode $queryType $core $platform
}

CommTestTask() {
#  input=(wb)
  input=(wb as soc-lj ok uk)
  querys=(triangle eTriangle square fourClique house threeTriangle solarSquare near5Clique)
#  querys=(triangle eTriangle square fourClique)
  method=MergedHCube
  isCommOnly=CommOnly
  taskNum=196
  mainClass=org.apache.spark.adj.utils.exp.ExpEntry
  numSamples=100000

  # shellcheck disable=SC2068
  for i in ${input[@]}; do
    data=$i
    file="${prefix}/${data}"
    for query in ${querys[@]}; do
      echo "----------------------------------"
      echo executing $i $query $k
      $executeScript --num-executors $numExecutors --class $mainClass $JAR -q ${query} -t $timeout -d ${file} -c $isCommOnly -m $method -n $taskNum -s $numSamples
    done
  done
}


echo "---------------DISC---------------"

#5NodePatternTask_Node
#5NodePatternTask_Edge
#6NodePatternTask
#DEBUGTask

#ScalabilityTask
#NodePairTask
#4ProfileTask

#PlanGenerationTask
#TriangleTask

#StatisticTask
#5EdgeTask
#5ProfileTask
#CommTestTask
#5NodePatternTask_Node

ExtraTask
