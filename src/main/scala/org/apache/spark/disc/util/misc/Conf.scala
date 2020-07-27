package org.apache.spark.disc.util.misc

import java.io.FileNotFoundException
import java.util.Properties

import scala.io.Source

class Conf() {

  //Environment Related Parameter
  var NUM_PARTITION = 4 //numbers of partitions (this is only a minimal value, the numbers of partition will automatically increase in case of large datasets)
  var NUM_MACHINE = 4 //numbers of machines
  var TIMEOUT = 43200 // timeout in terms of seconds
  var HCUBE_MEMORY_BUDGET = 5 * Math.pow(10, 8) //memory budget allocated for each partition in terms of Bytes
  var IS_YARN = false // whether running the system on yarn or locally

  //Query Related Parameters
  var data = ""
  var query = ""
  var core = "A"
  var queryType = QueryType.ISO
  var executionMode = ExecutionMode.Count
  var cacheSize = 10000000

  def load() = {
    val url = "disc.properties"
    val properties: Properties = new Properties()

    if (url != null) {
      val source = Source.fromFile(url)
      properties.load(source.bufferedReader())
    } else {
      throw new FileNotFoundException("Properties file cannot be loaded")
    }

    NUM_PARTITION = properties.getProperty("NUM_PARTITION").toInt
    NUM_MACHINE = properties.getProperty("NUM_MACHINE").toInt
    TIMEOUT = properties.getProperty("TIMEOUT").toInt
    HCUBE_MEMORY_BUDGET = properties
      .getProperty("HCUBE_MEMORY_BUDGET")
      .toDouble * Math.pow(10, 6)
    IS_YARN = properties.getProperty("IS_YARN").toBoolean
  }

  def setCluster() = {
    //For Cluster
    NUM_PARTITION = 7 * 28
    NUM_MACHINE = 7 * 28
    IS_YARN = true
  }

  def setLocalCluster() = {
    //For Parallel
    NUM_PARTITION = 16
    NUM_MACHINE = 16
    IS_YARN = false
  }

  def setOneCoreLocalCluster() = {
    //For Single
    NUM_PARTITION = 1
    NUM_MACHINE = 1
    IS_YARN = false
  }
}

object Conf {
  lazy val conf = {
    val _conf = new Conf()
    _conf.load()
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
  val ShowPlan, CommOnly, Count =
    Value
}
