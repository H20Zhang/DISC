package org.apache.spark.adj.utils.misc

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.configuration.ConfigurationFactory
import org.apache.spark.adj.utils.misc.Conf.Method

import scala.collection.mutable

class Conf() {

  var taskNum = 224
  var method = Method.PullHCube

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
    val PushHCube, PullHCube, Factorize, MergedHCube, ADJ = Value
  }

//  def loadConf(path: String) = {
//    new Conf(ConfigFactory.parseFile(new File(path)))
//  }

}
