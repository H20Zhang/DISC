package org.apache.spark.adj.utils.misc

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.configuration.ConfigurationFactory
import org.apache.spark.adj.utils.misc.Conf.{Method, Mode}

import scala.collection.mutable

class Conf() {

  //For Single Machine
  var taskNum = 4
  var numMachine = 4
  var defaultNumSamples = 100
  var commSpeed = 1 * Math.pow(10, 7)
  var ADJHCubeMemoryBudget = 5 * Math.pow(10, 8)
  var isYarn = false

  var method = Method.UnOptimizedHCube
  var query = ""
  var timeOut = 43200
  var mode = Mode.Count
  var data = ""
  var totalCacheSize = 100000

  val partitionSpeed = numMachine * Math.pow(10, 5)

  //in terms of cardinality
  val mergeHCubeMemoryBudget = 5 * Math.pow(10, 7)
  val pushHCubeMemoryBudget = 5 * Math.pow(10, 7)

  def getTaskNum(): Int = taskNum
  def getMethod() = method
  def setCluster() = {
    //For Cluster
    defaultNumSamples = 100000
    taskNum = 7 * 28
    numMachine = 7 * 28
    commSpeed = 1 * Math.pow(10, 9)
    isYarn = true
    //  in terms of real size
    ADJHCubeMemoryBudget = 4.5 * Math.pow(10, 8)
  }

  def setLocalCluster() = {
    //For Cluster
    defaultNumSamples = 100000
//    taskNum = 28
//    numMachine = 28
    taskNum = 8
    numMachine = 8
    commSpeed = 1 * Math.pow(10, 9)
    isYarn = false
    //  in terms of real size
    ADJHCubeMemoryBudget = 4.5 * Math.pow(10, 8)
  }

  def setOneCoreLocalCluster() = {
    //For Cluster
    defaultNumSamples = 100000
    taskNum = 1
    numMachine = 1
    commSpeed = 1 * Math.pow(10, 9)
    isYarn = false
    //  in terms of real size
    ADJHCubeMemoryBudget = 4.5 * Math.pow(10, 8)
  }
}

object Conf {
  lazy val conf = {
    new Conf()
  }

  def defaultConf() = {
    conf
  }

  object Method extends Enumeration {
    type Method = Value
    val SPARKSQL, UnOptimizedHCube, PushHCube, PullHCube, Factorize,
    MergedHCube, ADJ, CacheHCube =
      Value
  }

  object Mode extends Enumeration {
    type Mode = Value
    val ShowPlan, CommOnly, Count =
      Value
  }

}
