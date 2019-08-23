package org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.Utility

import org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.Structure.{GHDNode, GHDTree}

class LogoAssembler(val tree:GHDTree, val nodeIdPrevOrder:Seq[(Int,Int,Int)], val p:Map[Int, Int], val lazyMapping:Map[Int, Boolean]) {


  def assemble():SubPattern = {
    val baseNode = tree.nodes(nodeIdPrevOrder.head._1)
    val baseSubPattern = baseNode.constructNodeWithP(p, GHDNode())
    var pattern = baseSubPattern


    def buildLazy(nodeId:Int, prevId:Int):SubPattern = {
      val orderGenerator = new LogoGJOrderGenerator(tree.nodes(nodeId))
      orderGenerator.setAdhensionPreference(tree.nodes(prevId))
      val order = orderGenerator.GJOrder()
      val stages = orderGenerator.GJStages()
      val logoConstructor = new LogoNodeConstructor(order, stages)
      logoConstructor.constructWithInitPattern(order.diff(pattern.attrIds()), pattern, p)
    }

    def buildEager(nodeId:Int, prevId:Int):SubPattern = {
      val subPattern = tree.nodes(nodeId).constructNodeWithP(p, GHDNode())
      pattern.build(subPattern)
    }

    nodeIdPrevOrder.drop(1).foreach{
      f => lazyMapping(f._1) match {
        case true => pattern = buildLazy(f._1, f._2)
        case false => pattern = buildEager(f._1, f._2)
      }
    }

    pattern
  }
}
