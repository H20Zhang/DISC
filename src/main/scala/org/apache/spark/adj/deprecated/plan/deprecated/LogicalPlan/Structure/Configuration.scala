package org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.Structure

class Configuration {
  val defaultP:Int = 11
  val defaultSampleP:Int = 6
  val defaultK = 10000l
  val defaultBase:Long = defaultK
  val defaultNetworkSpeed = 2000000000l
  val defaultMem = 10000000l
  val defaultMachines = 224
  val minimumTasks:Long = defaultMachines

}

object Configuration {
  lazy val configuration = new Configuration

  def getConfiguration():Configuration = {
    configuration
  }
}
