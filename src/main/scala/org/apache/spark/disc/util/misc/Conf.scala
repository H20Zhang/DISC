package org.apache.spark.disc.util.misc

import java.io.FileNotFoundException
import java.util.Properties

import scala.io.Source

case class Conf() {

  //Environment Related Parameter
  var NUM_PARTITION = 4 //numbers of partitions (this is only a minimal value, the numbers of partition will automatically increase in case of large datasets)
  var NUM_CORE = 4 //numbers of cores
  var TIMEOUT = 43200 // timeout in terms of seconds
  var HCUBE_MEMORY_BUDGET = 5 * Math.pow(10, 8) //memory budget allocated for each partition in terms of Bytes
  var IS_YARN = false // whether running the system on yarn or locally
  var CACHE_SIZE = 10000000 //default cache size for LRU

  //Query Related Parameters
  var data = ""
  var output = ""
  var query = ""
  var orbit = "A"
  var queryType = QueryType.ISO
  var executionMode = ExecutionMode.Count


  def load(url: String) = {
    val properties: Properties = new Properties()

    if (url != null) {
      val source = Source.fromFile(url)
      properties.load(source.bufferedReader())
    } else {
      throw new FileNotFoundException("Properties file cannot be loaded")
    }

    NUM_PARTITION = properties.getProperty("NUM_PARTITION").toInt
    NUM_CORE = properties.getProperty("NUM_CORE").toInt
    TIMEOUT = properties.getProperty("TIMEOUT").toInt
    HCUBE_MEMORY_BUDGET = properties
      .getProperty("HCUBE_MEMORY_BUDGET")
      .toDouble * Math.pow(10, 6)
    IS_YARN = properties.getProperty("IS_YARN").toBoolean
    CACHE_SIZE = properties.getProperty("CACHE_SIZE").toInt
  }
}

object Conf {
  lazy val conf = {
    val _conf = new Conf()
//    val url = "disc_local.properties"
//    _conf.load(url)
    _conf
  }

  def defaultConf() = {
    conf
  }
}

object QueryType extends Enumeration {
  type QueryType = Value
  val InducedISO, ISO, HOM, Debug =
    Value
}

object ExecutionMode extends Enumeration {
  type ExecutionMode = Value
  val ShowPlan, Count, Result, Exec, Debug =
    Value
}
