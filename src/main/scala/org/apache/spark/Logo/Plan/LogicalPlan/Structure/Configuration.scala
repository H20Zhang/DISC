package org.apache.spark.Logo.Plan.LogicalPlan.Structure

class Configuration {
  val defaultP:Int = 11
  val defaultSampleP:Int = 6
  val defaultK = 1000l
  val defaultBase = defaultK
  val defaultNetworkSpeed = 2000000000l
  val defaultMem = 100000l
  val minimumTasks = Math.pow(defaultP,3) - 1
  val defaultMachines = 224
}

object Configuration {
  lazy val configuration = new Configuration

  def getConfiguration():Configuration = {
    configuration
  }
}
