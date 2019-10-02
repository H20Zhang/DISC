package org.apache.spark.adj.utils.misc

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.configuration.ConfigurationFactory
import org.apache.spark.adj.utils.misc.Conf.Method

import scala.collection.mutable

class Conf() {

  //For Single Machine
//  var taskNum = 4
//  var numMachine = 4
//  var defaultNumSamples = 100
//  val commSpeed = 1 * Math.pow(10, 7)
//  val ADJHCubeMemoryBudget = 5 * Math.pow(10, 8)

  //For Cluster
  var defaultNumSamples = 100000
  var taskNum = 7 * 28
  var numMachine = 7 * 28
  val commSpeed = 1 * Math.pow(10, 9)

//  in terms of real size
  val ADJHCubeMemoryBudget = 4.5 * Math.pow(10, 8)

  var method = Method.UnOptimizedHCube
  var query = ""
  var timeOut = 43200
  var commOnly = false
  var data = ""
  var totalCacheSize = 100000
  var isYarn = false
  val partitionSpeed = numMachine * Math.pow(10, 5)

  //in terms of cardinality
  val mergeHCubeMemoryBudget = 5 * Math.pow(10, 7)
  val pushHCubeMemoryBudget = 3 * Math.pow(10, 7)

  def getTaskNum(): Int = taskNum
  def getMethod() = method
}

object Conf {
  lazy val conf = {
    new Conf()
//    loadConf("./src/main/scala/org/apache/spark/adj/utils/misc/default.conf")
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

//  def loadConf(path: String) = {
//    new Conf(ConfigFactory.parseFile(new File(path)))
//  }

}
