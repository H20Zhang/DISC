package org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize

import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{Configuration, GHDPlan, GHDTree}
import org.apache.spark.Logo.Plan.LogicalPlan.Utility.InformationSampler
import org.apache.spark.Logo.UnderLying.utlis.ListGenerator

class GHDPlanOptimizer(val tree:GHDTree) {

  val configuration = Configuration.getConfiguration()
  val defaultP:Int = configuration.defaultP
  val defaultSampleP:Int = configuration.defaultSampleP
  val defaultK = configuration.defaultK
  val informationSampler = new InformationSampler(tree, defaultK)

  def genOrders():Seq[Seq[Int]] ={

    val nodeIds = tree.nodes.keys.toSeq
    val validOrders = nodeIds.permutations.toSeq.filter{
      p =>
        val prefixs = 0 to p.size
        prefixs.map(p.take(_)).forall(p => tree.graph.nodeInducedSubgraph(p).isConnected())
    }

    validOrders
  }

  def genLazyMappings():Seq[Map[Int,Boolean]] = {
    val idList = tree.nodes.keys.toSeq
    val isLazyList = ListGenerator.cartersianSizeList(ListGenerator.fillList(2,idList.size)).map(g => g.map(h => h == 1))
    isLazyList.map(f => idList.zip(f).toMap)
  }

    //here we only consider attr can take 1 or default P which is a simplified version of HyberCube's P calculation
    def genPs():Seq[Map[Int,Int]] = {
      val pList = ListGenerator
        .cartersianSizeList(
          ListGenerator
            .fillList(2,tree.attributes.size))
        .map(g => g.map(h => h == 1))
        .map(f => f.map{g =>
          g match {
            case true => defaultP
            case false => 1
          }})
      val res = pList.map(f => tree.attributes.zip(f).toMap).filter(p => p.values.product > configuration.minimumTasks)
      res
    }

  def genPlans() = {
    val orders = genOrders()
    val lazyMappings = genLazyMappings()
    val ps = genPs()
    val possibleConfigurations =
      orders
      .flatMap(f => lazyMappings.map(g => (f,g)))
      .flatMap(f => ps.map(g => (f._1, g, f._2)))

    val possiblePlans = possibleConfigurations.map(f => GHDPlan(tree, f._1, f._2, f._3, informationSampler))
    possiblePlans
  }

  def genBestPlan():(GHDPlan,Long) = {
    val plans = genPlans()
    plans.map(f => (f,f.costEstimation())).minBy(_._2)
  }

}

