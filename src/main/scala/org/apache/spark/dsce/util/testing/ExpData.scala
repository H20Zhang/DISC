package org.apache.spark.dsce.util.testing

object ExpData {

  val prefix = "./examples/"

  val graphDataAdresses = Map(
    ("eu", "email-Eu-core.txt"),
    ("wikiV", "wikiV.txt"),
    ("debug", "debugData.txt")
  )

  def getDataAddress(data: String) = {
    prefix + graphDataAdresses(data)
  }

}
