package org.apache.spark.adj.utils.misc

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.configuration.ConfigurationFactory

import scala.collection.mutable

class Conf(conf: Config) {
  def taskNum(): Int = conf.getInt("task.num")
}

object Conf {
  lazy val conf = {
    loadConf("./src/main/scala/org/apache/spark/adj/conf/default.conf")
  }

  def defaultConf() = {
    conf
  }

  def loadConf(path: String) = {
    new Conf(ConfigFactory.parseFile(new File(path)))
  }

}
