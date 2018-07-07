package org.apache.spark.Logo.Plan.LogicalPlan.Utility

import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{GHDNode, Relation, RelationSchema}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//default preference is 10, the lower, the more preferable
class LogoGJOrderGenerator(cur:GHDNode) {

  lazy val relationSchema = RelationSchema.getRelationSchema()

  var preference:Map[Int,Int] = cur.intraNodeRelationGraph().nodes().map((_,10)).toMap

  def setAdhensionPreference(prev:GHDNode):Unit = {
    val preferedAttrs = prev.intersect(cur)._1

    preference = preference.map{
      case(key,value) => preferedAttrs.contains(key) match {
        case true => (key, value - 1)
        case false => (key, value)
      }
    }
  }

  def setPreference(manualPreference:Map[Int,Int]):Unit = {
    preference = preference.map{
      case(key,value) => manualPreference.contains(key) match {
        case true => (key, manualPreference(key))
        case false => (key, value)
      }
    }
  }


  def GJFirstRelation():Relation = {
    val firstAttrs = GJOrder().take(2)
    val relationID = relationSchema.getRelationId((firstAttrs(0),firstAttrs(1))).get
    relationSchema.getRelation(relationID)
  }

  //here for node, connected > disconnected, adhension > not adhension
  def GJOrder():Seq[Int] = {

    val intraNodeGraph = cur.intraNodeRelationGraph()
    val visited = ArrayBuffer[Int]()
    var candidates = intraNodeGraph.nodes().map(f => (f,preference(f))).sortBy(_._2)
    var nexts: ArrayBuffer[Int] = ArrayBuffer[Int]()

    nexts.append(candidates.head._1)

    while (!nexts.isEmpty) {
      val v = nexts.map(f => (f,preference(f))).sortBy(_._2).head._1

      nexts -= v
      if (!visited.contains(v)) {
        visited += v

        nexts.appendAll(intraNodeGraph.getNeighbors(v))
      }
    }

    visited
  }


  def GJStages():Map[Int,Seq[Relation]] = {
    val orders = GJOrder()
    val stagesMap = mutable.Map[Int,Seq[Relation]]()
    val relations = (1 to orders.size).map{f =>
      val prefixOrders = orders.take(f)
      val prefixOrderLastElement = prefixOrders.last
      val prefixOrderExecptLast = prefixOrders.dropRight(1)
      val possibleTuples = prefixOrderExecptLast.map(f => (f,prefixOrderLastElement))
      possibleTuples.map(relationSchema.getRelationId).filter(_.isDefined).map(_.get).map(relationSchema.getRelation)
    }

    orders.zip(relations).foreach(f => stagesMap.put(f._1,f._2))
    stagesMap.toMap
  }
}
