package org.apache.spark.disc.testing

object ExpData {

  val prefix = "./examples/"

  val graphDataAdresses = Map(
    ("eu", "email-Eu-core.txt"),
    ("wv", "wikiV.txt"),
    ("facebook", "facebook.txt"),
    ("reactcome", "reactcome.txt"),
    ("as-caida", "as-caida.txt"),
    ("to", "topology.txt"),
    ("debug", "debugData2.txt")
  )

  def getDataAddress(data: String) = {
    prefix + graphDataAdresses(data)
  }

}
