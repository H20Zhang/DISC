package org.apache.spark.Logo.Plan.LogicalPlan.Structure

import org.apache.log4j.LogManager
import org.apache.spark.Logo.Plan.LogicalPlan.Utility.{AGMSolver, LogoGJOrderGenerator, LogoNodeConstructor, SubPattern}
import org.apache.spark.Logo.UnderLying.utlis._

import scala.collection.mutable.ArrayBuffer

class GHDNode(val id:Int, var relationIDs:ArrayBuffer[Int], val attributeIDs:ArrayBuffer[Int], var nexts:Seq[TreeNode]) extends TreeNode{
  override def children(): Seq[TreeNode] = nexts

  val log = LogManager.getLogger(this.getClass)
  lazy val relationSchema = RelationSchema.getRelationSchema()

  def toCompleteAttributeNode() = {
    val relationSchema = RelationSchema.getRelationSchema()
    val attributes = relations.flatMap(_.attributes).distinct
    val inducedRelations = relationSchema.getInducedRelation(attributes)

    relationIDs = ArrayBuffer() ++ inducedRelations
  }

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

  def estimatedAGMCardinality() = {
    val agmResult = AGMSolver.solveAGMBound(relationIDs)

    val fractioalCover = AGMSolver.AGMOptimalFractionEdgeCover(relationIDs).toList

    //TODO remember to change back after testing
//    println(fractioalCover)
    (agmResult,fractioalCover.sum)
  }

  def sampledGJCardinality(k:Long, prev:GHDNode) = {
    log.warn(s"start sampling cardinality for node ${id}")
    val orderGenerator = new LogoGJOrderGenerator(this)
    val order = orderGenerator.GJOrder()
    val stages = orderGenerator.GJStages()
    orderGenerator.setAdhensionPreference(prev)

    val constructor = new LogoNodeConstructor(order, stages)
    val subPattern = constructor.constructSampleLogoWithEdgeLimit(k)
    val time_size_pair = subPattern.logo.time_size()

    //TODO this place may need some change
    val mul = relationSchema.getRelation((order(0),order(1))).get .toDouble/Configuration.getConfiguration().defaultK
    //size, time
    ((time_size_pair._1 * mul) toLong, (time_size_pair._2 * mul) toLong, time_size_pair._3)
  }

  def sampleOfEdgeTuple(sampleSize:Long, attrNodeIDs:(Int,Int), preference:Map[Int,Int]):SubPattern = {
    log.warn(s"generating sampled edges from node ${id}")
    val orderGenerator = new LogoGJOrderGenerator( this)
    orderGenerator.setPreference(preference)
    val logoConstructor = new LogoNodeConstructor(orderGenerator.GJOrder(), orderGenerator.GJStages())
    logoConstructor.initSampledPatternFromAttrTuple(sampleSize, attrNodeIDs)
  }


  def sampledQueryTime(k:Long, prev:GHDNode) = {
    log.warn(s"start sampling process from sampled edge from node:${prev} for node:${id}")
    val orderGenerator = new LogoGJOrderGenerator(this)
    orderGenerator.setAdhensionPreference(prev)
    val logoConstructor = new LogoNodeConstructor(orderGenerator.GJOrder(), orderGenerator.GJStages())
    val adhension = prev.intersect(this)._1

    //TODO make this place more complete
    assert(adhension.size == 2)

    val adhensionTuple = (adhension(0), adhension(1))
    val sampledPrevPattern = prev.sampleOfEdgeTuple(k, adhensionTuple, adhension.map((_,20)).toMap)


    val subPattern = logoConstructor.constructSampleLogoWithInitPattern(orderGenerator.GJOrder().diff(adhension),sampledPrevPattern)

    subPattern.logo.time_size()
  }

  def constructNodeWithP(p:Map[Int,Int], prev:GHDNode) = {
      val orderGenerator = new LogoGJOrderGenerator(this)
      orderGenerator.setAdhensionPreference(prev)

      val constructor = new LogoNodeConstructor(orderGenerator.GJOrder(), orderGenerator.GJStages())
      val subPattern = constructor.constructPattern(p)

      subPattern
  }

  override def toString: String = {
    s"${this.getClass.getSimpleName} id:${id} relations:${relations}"
  }

  def shortString: String = {
    attributeIDs.map(relationSchema.getAttribute).toString()
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
