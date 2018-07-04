package org.apache.spark.Logo.Plan.LogicalPlan.Structure

import org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize.AGMSolver
import org.apache.spark.Logo.Plan.LogoRDDReference
import org.apache.spark.Logo.UnderLying.utlis.{ImmutableGraph, LogoConstructor}
import spire.syntax.order

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GHDNode(val id:Int, var relationIDs:ArrayBuffer[Int], val attributeIDs:ArrayBuffer[Int], var nexts:Seq[TreeNode]) extends TreeNode{
  override def children(): Seq[TreeNode] = nexts

  lazy val relationSchema = RelationSchema.getRelationSchema()

  def relations = {
    val relationSchema = RelationSchema.getRelationSchema()
    val relations = relationIDs.map(relationSchema.getRelation)
    relations
  }

  def isConnected():Boolean = {

    relations.foreach{
      f =>

        val filteredRelations = relations.filter(_ != f)

        if (filteredRelations.size != 0){
          val res = filteredRelations.forall{
            t =>
              f.attributes.intersect(t.attributes).isEmpty
          }

          if (res){
            return false
          }
        }

    }

    true
  }

  //here, we assume the relation is binary relations
  def intraNodeRelationGraph():ImmutableGraph = {
    val relationSchema = RelationSchema.getRelationSchema()
    val edges = relations.map(_.attributes).map(f => (relationSchema.getAttributeId(f(0)),relationSchema.getAttributeId(f(1))))
    ImmutableGraph(edges)
  }

  def intersect(rhs:GHDNode) = {
    (attributeIDs.intersect(rhs.attributeIDs), (relationIDs.intersect(rhs.relationIDs)))
  }

  def contains(rhs:GHDNode):Boolean = {
    rhs.relationIDs.diff(relationIDs).isEmpty
  }

  def estimatedCardinality() = {
    val agmResult = AGMSolver.solveAGMBound(relationIDs)

    val fractioalCover = AGMSolver.AGMOptimalFractionEdgeCover(relationIDs).toList

    //TODO remember to change back after testing
//    println(fractioalCover)
    (agmResult,fractioalCover.sum)
  }

  def toCompleteAttributeNode() = {
    val relationSchema = RelationSchema.getRelationSchema()
    val attributes = relations.flatMap(_.attributes).distinct
    val inducedRelations = relationSchema.getInducedRelation(attributes)

    relationIDs = ArrayBuffer() ++ inducedRelations
  }

  override def toString: String = {
    s"${this.getClass.getSimpleName} id:${id} relations:${relations}"
  }

  def toOrderedGHDNode(order:Int):OrderedGHDNode = {
    OrderedGHDNode(this, order)
  }
}

object GHDNode{
  private var nodeCount = 0
  def apply(relationIDs:ArrayBuffer[Int]):GHDNode = {
    nodeCount += 1

    val relationSchema = RelationSchema.getRelationSchema()
    val relations = relationIDs.map(relationSchema.getRelation)
    val attributeIDs = relations.flatMap(_.attributes).distinct.map(relationSchema.getAttributeId)

    new GHDNode(nodeCount-1,relationIDs, attributeIDs, Seq())
  }

  def apply():GHDNode = {
    nodeCount += 1

    new GHDNode(nodeCount-1,ArrayBuffer(),ArrayBuffer(),Seq())
  }
}

class OrderedGHDNode(val order:Int, id:Int, relationIDs:ArrayBuffer[Int], attributeIDs:ArrayBuffer[Int], var prev:OrderedGHDNode) extends GHDNode(id,relationIDs,attributeIDs, Seq(prev)) {
  override def children(): Seq[TreeNode] = Seq(prev)

  var sampledCardinality:Double = _
  var sampledExecutionTime:Double = _
  var sampledQueryTime:Double = _


  def setPrev(node:OrderedGHDNode): Unit ={
    prev = node
  }

  override def toString: String = {
    prev == null match {
      case true => s"${this.getClass.getSimpleName} id:${id} relations:${relations} $order noPrev"
      case false => s"${this.getClass.getSimpleName} id:${id} relations:${relations} $order ${prev.id}"
    }

  }

  //sampling related
  def adhension():Seq[Int] = prev == null match {
    case true => Seq()
    case false => prev.intersect(this)._1
  }




  //here for node, connected > disconnected, adhension > not adhension
  def GJOrder():Seq[Int] = adhension().isEmpty match {
    case true => {
      val intraNodeGraph = intraNodeRelationGraph()
      val visited = ArrayBuffer[Int]()
      val head = intraNodeGraph.nodes().head

      var nexts: ArrayBuffer[Int] = ArrayBuffer[Int]()
      nexts.append(head)

      while (!nexts.isEmpty) {
        val v = nexts.head

        nexts.remove(0)
        if (!visited.contains(v)) {
          visited += v

          nexts.appendAll(intraNodeGraph.getNeighbors(v))
        }
      }

      visited
    }
    case false => {

      val intraNodeGraph = intraNodeRelationGraph()
      val visited = ArrayBuffer[Int]()
      val adhensionArray = adhension()
      val head = intraNodeGraph.nodes().filter(adhensionArray.contains).head

      var nexts: ArrayBuffer[Int] = ArrayBuffer[Int]()

//      println(intraNodeGraph)

      nexts.append(head)

      while (!nexts.isEmpty) {


        var v: Int = 0


        nexts.filter(adhensionArray.contains).isEmpty match {
          case true => v = nexts.head
          case false => v = nexts.filter(adhensionArray.contains).head
        }

//        println(v)
        //        val v = nexts.head

        nexts -= v
        if (!visited.contains(v)) {
          visited += v

          nexts.appendAll(intraNodeGraph.getNeighbors(v))
        }
      }


      visited
    }

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

  def samplingLogo(sampleSize:Long):LogoRDDReference = {
    val logoConstructor = new LogoConstructor(GJOrder(),GJStages(), 6)
    logoConstructor.constructSampleLogo()
  }


  def querySamplingLogo(sampleSize:Long):LogoRDDReference = ???
}

object OrderedGHDNode{
  def apply(node:GHDNode, order:Int) ={
    new OrderedGHDNode(order, node.id, node.relationIDs, node.attributeIDs, null)
  }
}

class ExecutionGHDNode(order:Int, id:Int, relationIDs:ArrayBuffer[Int], attributeIDs:ArrayBuffer[Int], prev:OrderedGHDNode, isLazy:Boolean, P:Map[Int,Int]) extends OrderedGHDNode(order,id,relationIDs,attributeIDs,prev) {

  def Logo():LogoRDDReference = ???
  def subPatternWithPrev():LogoRDDReference = ???
}

object ExecutionGHDNode{

}
