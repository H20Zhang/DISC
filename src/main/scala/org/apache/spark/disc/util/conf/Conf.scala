package org.apache.spark.disc.util.conf

class Conf() {

  var taskNum = 4
  var numMachine = 4
  var defaultNumSamples = 100
  var commSpeed = 1 * Math.pow(10, 7)
  var isYarn = false
  var query = ""
  var timeOut = 43200
  var data = ""
  var totalCacheSize = 100000
  val partitionSpeed = numMachine * Math.pow(10, 5)
  val HCubeMemoryBudget = 5 * Math.pow(10, 7)
  var queryType = QueryType.NonInduce
  var executionMode = ExecutionMode.Count
  var cacheSize = 10000000
  var core = "A"

  def getTaskNum(): Int = taskNum
  def setCluster() = {
    //For Cluster
    defaultNumSamples = 100000
    taskNum = 7 * 28
    numMachine = 7 * 28
    commSpeed = 1 * Math.pow(10, 9)
    isYarn = true

  }

  def setLocalCluster() = {
    //For Parallel
    defaultNumSamples = 100000
    taskNum = 16
    numMachine = 16
    commSpeed = 1 * Math.pow(10, 9)
    isYarn = false
  }

  def setOneCoreLocalCluster() = {
    //For Single
    defaultNumSamples = 100000
    taskNum = 1
    numMachine = 1
    commSpeed = 1 * Math.pow(10, 9)
    isYarn = false
  }
}

object QueryType extends Enumeration {
  type QueryType = Value
  val Induce, NonInduce, Partial, Debug =
    Value
}

object ExecutionMode extends Enumeration {
  type ExecutionMode = Value
  val ShowPlan, CommOnly, Count =
    Value
}
