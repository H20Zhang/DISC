#!/usr/bin/env bash

prefix="/hzhang/data"
JAR="./ADJ-assembly-0.1.1.jar"

Test_HCUBEGJ() {

  mainClass=org.apache.spark.adj.utils.exp.ExpEntry
  #input=(as lj webB wikiT  enwiki-2013 orkut)
  input=(as)
#  querys=(triangle fourClique l31 l32 b313 house near5Clique solarSquare)
  querys=(house)
#  methods=(PushHCube MergedHCube PullHCube Factorize)
  methods=(PushHCube)
  timeout=86400
  executeScript=runSpark-logo.sh
  #executeScript=runSpark-logo-local.sh
  #isCommOnly=true
  isCommOnly=false
  #method=Factorize
  #method=PullHCube
  #method=MergedHCube
  #method=PushHCube
  # shellcheck disable=SC2068
  for i in ${input[@]}; do
    data=$i
    file="${prefix}/${data}"
    for query in ${querys[@]}; do
      for method in ${methods[@]}; do
        echo "----------------------------------"
        echo executing $i $query $k

        SECONDS=0
        $executeScript --num-executors 32 --class $mainClass $JAR -q ${query} -t $timeout -d ${file} -c $isCommOnly -m $method
        duration=$SECONDS
        echo "executing $executeScript  --num-executors 32  --class $mainClass $JAR -q ${query} -t $timeout -d ${file} -c $isCommOnly -m $method with $duration seconds elapsed."

      done
    done
  done
}

Test_HCUBEGJ
