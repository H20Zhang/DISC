package org.apache.spark.adj.utils.misc

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.configuration.ConfigurationFactory
import org.apache.spark.adj.utils.misc.Conf.Method

import scala.collection.mutable

class Conf() {

  var taskNum = 7 * 28
//  var taskNum = 4
  var method = Method.UnOptimizedHCube
  var query = ""
  var timeOut = 43200
  var commOnly = false
  var data = ""
  var totalCacheSize = 100000
  var isYarn = false

  def getTaskNum(): Int = taskNum
  def getMethod() = method
//    conf.getInt("taskNum")
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
    val UnOptimizedHCube, PushHCube, PullHCube, Factorize, MergedHCube, ADJ,
    CacheHCube =
      Value
  }

//  def loadConf(path: String) = {
//    new Conf(ConfigFactory.parseFile(new File(path)))
//  }

}
