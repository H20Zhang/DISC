package org.apache.spark.adj.utils.testing

object TestingHelper {


  def genGraphContent(name:String) = ContentGenerator.loadGraphContent(name)

  def genRandomContent(cardinality:Int, artiy:Int) = ContentGenerator.genRandomContent(cardinality, artiy)

  def genIdentityContent(cardinality:Int, artiy:Int) = ContentGenerator.genIdentityContent(cardinality, artiy)
}
