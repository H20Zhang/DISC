package org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.Decrapted.CostBasedOptimizer




case class QueryCost(val costs:Map[(Int,Int),Double]){
  def getCost(q:(Int,Int)):Double = {
    costs(q)
  }
}

case class SubPatternStructure(val nodes:Seq[Int], val edges:Seq[(Int,Int)])

class SubPattern(val id:Int,
                      val structure:SubPatternStructure,
                      val lazyCardinality:Double,
                      val cardinality:Double) {

}

case class OrderedSubPattern(subPattern:SubPattern,
                             order:Int,
                             isLazy:Boolean) extends SubPattern(subPattern.id,subPattern.structure,subPattern.lazyCardinality, subPattern.cardinality){
  def commTime(speed:Double) = isLazy match {
    case false => (cardinality * structure.nodes.size * 4) / (speed * Math.pow(10,3))
    case true => (lazyCardinality * 2 * 4) / (speed * Math.pow(10,3)) //lazy pattern is constructed using edges
  }

  def compTime(prevSubPattern:OrderedSubPattern, qCost:QueryCost, memLimit:Double, core:Int, offsetFix:Double, speed:Double) = isLazy match {
    case true => (qCost.getCost(prevSubPattern.id,id) * prevSubPattern.cardinality) / (core*offsetFix)
    case false => (commTime(speed) + qCost.getCost(prevSubPattern.id,prevSubPattern.id)  * Math.ceil(cardinality/memLimit)) / (core*offsetFix)
  }

}
