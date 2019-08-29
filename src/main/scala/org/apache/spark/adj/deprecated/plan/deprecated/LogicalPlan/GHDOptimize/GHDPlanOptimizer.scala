package org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.GHDOptimize

import org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.Structure.{Configuration, GHDPlan, GHDTree, RelationSchema}
import org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.Utility.InformationSampler
import org.apache.spark.adj.deprecated.utlis.ListGenerator

class GHDPlanOptimizer(val tree:GHDTree, val informationSampler1: InformationSampler = null) {

  val relationSchema = RelationSchema.getRelationSchema()
  val configuration = Configuration.getConfiguration()
  val defaultP:Int = configuration.defaultP
  val defaultSampleP:Int = configuration.defaultSampleP
  val defaultK = configuration.defaultK
  var informationSampler = informationSampler1 == null match{
    case true => new InformationSampler(tree, defaultK)
    case false => informationSampler1
  }

  def setInformationSampler(informationSampler2: InformationSampler) = {
    this.informationSampler = informationSampler2
  }

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
            .fillList(2,tree.attributeIDs.size))
        .map(g => g.map(h => h == 1))
        .map(f => f.map{g =>
          g match {
            case true => defaultP
            case false => 1
          }})
      val res = pList.map(f => tree.attributeIDs.zip(f).toMap)
      val relationWithAttrs = tree.relationIDs
        .map(relationSchema.getRelation)
        .map(f => (f,f.attrIDs))

      res
        .filter(p => p.values.product >= configuration.minimumTasks)
        .filter(p => relationWithAttrs.forall(o => o._2.map(p).product >= configuration.defaultP))

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

  def genBestPlan():(GHDPlan,(Double,Double)) = {
    val plans = genPlans()
    val minPrimaryCost = plans.map(f => (f,f.costEstimation())).minBy(_._2._1)
    val filteredPlans = plans.filter(p => p.costEstimation()._1 == minPrimaryCost._2._1)
    filteredPlans.map(f => (f,f.costEstimation())).minBy(_._2._2)
  }

}

