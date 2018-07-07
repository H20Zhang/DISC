package org.apache.spark.Logo.Plan.LogicalPlan.Structure

class Configuration {
  val defaultP:Int = 11
  val defaultSampleP:Int = 6
  val defaultK = 1000000l
  val defaultBase = defaultK
  val defaultNetworkSpeed = 2000000000l
}

object Configuration {
  lazy val configuration = new Configuration

  def getConfiguration():Configuration = {
    configuration
  }
}
