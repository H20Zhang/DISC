package org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize

import org.apache.spark.Logo.Plan.LogicalPlan.Structure.LogicalTreeNode

import scala.collection.mutable.ArrayBuffer


@deprecated
class GHDOptimizer(relation:ArrayBuffer[Int]) {

  lazy val subSetsGenerator = new subSetGenerator(relation)
  lazy val gHD2TreeConverter = new GHD2TreeConverter

  def allLeftDeepTrees():Seq[LogicalTreeNode] = {
    val ghds = subSetsGenerator.optimalGHDSets()
    val trees = ghds.map(_._1.map(_._1)).flatMap(gHD2TreeConverter.GHD2LeftDeepTree)
    trees
  }

  def sampleLeftDeepTrees():Seq[LogicalTreeNode] = {
    val sampleGHD = subSetsGenerator.optimalGHDSets().head._1.map(_._1)
    val trees = gHD2TreeConverter.GHD2LeftDeepTree(sampleGHD)

    trees
  }



}
