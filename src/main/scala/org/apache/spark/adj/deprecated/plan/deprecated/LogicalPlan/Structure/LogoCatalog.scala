package org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.Structure

import org.apache.spark.adj.deprecated.plan.deprecated.PhysicalPlan.Logo
import org.apache.spark.adj.deprecated.execution.rdd.loader.{DataLoader, EdgeLoader}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * logoCatlog used to associate a name with a RDD[LogoRDDReference],
  * LogoRDDReference records the RDD[LogoBlock] and its according schema
  */
class LogoCatalog {
  val relationIDPMap: mutable.Map[(Int,(Int,Int)), (Logo,Long)] = mutable.Map()
  val relationIDMap:mutable.Map[Int, (RDD[(Array[Int],Int)],Long)] = mutable.Map()
  val addressMap: mutable.Map[String, (RDD[(Array[Int],Int)],Long)] = mutable.Map()
//  val sampledRDDMap: mutable.Map[RelationWithP, PatternLogoRDDReference] = mutable.Map()


  val relationSchema = RelationSchema.getRelationSchema()

  def relationIDPPairGen(relation:RelationWithP) = {
    (relationSchema.getRelationId(relation.originalRelation), (relation.p(0), relation.p(1)))
  }

  def registorRelationWithP(relation: RelationWithP):(Logo,Long) = {

    val IDPpair = relationIDPPairGen(relation)

    val (rawEdge,resCount) = retrieveOrRegisterRelation(relation.originalRelation)
    val edgeLoader = new EdgeLoader(rawEdge, Seq(relation.p(0), relation.p(1)))
    val resEdge = edgeLoader.edgeLogoRDDReference
    relationIDPMap(IDPpair) = (resEdge, resCount)

    (resEdge, resCount)
  }

  def registorRelation(relation: Relation) = {

    val (rawEdge,count) = retrieveOrRegistorAddress(relation.address)

    relationIDMap(relationSchema.getRelationId(relation)) = (rawEdge,count)
    relation.cardinality = count
    (rawEdge, count)
  }

  def registorAdress(address:String) = {
    val rawEdge = new DataLoader(address) rawEdgeRDD
    val count = rawEdge.count()

    addressMap(address) = (rawEdge,count)
    (rawEdge, count)
  }

  def getSampledRelationWithP(relation: RelationWithP, k:Long):Logo = {

    val rawEdge = new DataLoader(relation.address) sampledRawEdgeRDD(k.toInt)

    val edgeLoader = new EdgeLoader(rawEdge, Seq(relation.p(0), relation.p(1)))
    val res = edgeLoader.edgeLogoRDDReference

    res
  }

  def retrieveOrRegistorAddress(address:String) = {
    addressMap.get(address) match {
      case Some(v) => v
      case None => registorAdress(address)
    }
  }

  def retrieveOrRegisterRelationWithP(relation:RelationWithP) = {
    relationIDPMap.get(relationIDPPairGen(relation)) match {
      case Some(v) => v._1
      case None => registorRelationWithP(relation)._1
    }
  }

  def retrieveOrRegisterRelation(relation: Relation) = {
    relationIDMap.get(relationSchema.getRelationId(relation)) match {
      case Some(v) => v
      case None => registorRelation(relation)
    }
  }

}

object LogoCatalog {
  lazy val _catalog = new LogoCatalog

  def getCatalog() = _catalog
}

