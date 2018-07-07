package org.apache.spark.Logo.Plan.LogicalPlan.Structure

import org.apache.spark.Logo.Plan.PhysicalPlan.Logo
import org.apache.spark.Logo.UnderLying.Loader.{EdgeLoader, EdgePatternLoader}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * logoCatlog used to associate a name with a RDD[LogoRDDReference],
  * LogoRDDReference records the RDD[LogoBlock] and its according schema
  */
class LogoCatalog {
  val rddMap: mutable.Map[RelationWithP, Logo] = mutable.Map()
//  val sampledRDDMap: mutable.Map[RelationWithP, PatternLogoRDDReference] = mutable.Map()
  val addressPMap: mutable.Map[(String,(Int,Int)), (Logo,Long)] = mutable.Map()
  val addressMap: mutable.Map[String, (RDD[(Array[Int],Int)],Long)] = mutable.Map()

  val relationSchema = RelationSchema.getRelationSchema()

  def addressPPairGen(relation:RelationWithP) = {
    (relation.address, (relation.p(0), relation.p(1)))
  }

  def registorRelationWithP(relation: RelationWithP):(Logo,Long) = {

//    println(s"registor ${relation}")

    val addressPPair = addressPPairGen(relation)

    val res = addressPMap.get(addressPPair) match {
      case Some(res) => res
      case None => {
//        println(s"init registor ${relation}")
        val (rawEdge,resCount) = registorRelation(relation.originalRelation)
        val edgeLoader = new EdgePatternLoader(rawEdge, Seq(relation.p(0), relation.p(1)))
        val resEdge = edgeLoader.edgeLogoRDDReference

        addressPMap(addressPPair) = (resEdge, resCount)

        (resEdge, resCount)
      }
    }

    linkRelationWithLogo(relation, res)
    res
  }

  def registorRelation(relation: Relation) = {
    val rawEdge = new EdgeLoader(relation.address) rawEdgeRDD
    val count = rawEdge.count()

    addressMap(relation.address) = (rawEdge,count)
    (rawEdge, count)
  }

  def getSampledRelationWithP(relation: RelationWithP, k:Long):Logo = {
    val rawEdge = new EdgeLoader(relation.address) sampledRawEdgeRDD(k.toInt)

    val edgeLoader = new EdgePatternLoader(rawEdge, Seq(relation.p(0), relation.p(1)))
    val res = edgeLoader.edgeLogoRDDReference

//    linkRelationWithSampledLogo(relation, res)

    res
  }

  def updateRelationCardinality(relation:RelationWithP, cardinality:Long) = {
    relation.originalRelation.cardinality = cardinality
  }

  def linkRelationWithLogo(relation:RelationWithP, logo:(Logo,Long)) = {
    updateRelationCardinality(relation, logo._2)
    rddMap.put(relation, logo._1)
  }

  def retrieveLogo(relation:RelationWithP) = rddMap.get(relation)

  def retrieveOrRegisterLogo(relation:RelationWithP) = {
    addressPMap.get(addressPPairGen(relation)) match {
      case Some(v) => v
      case None => registorRelationWithP(relation)._1
    }
  }

  def retrieveOrRegisterRelation(relation: Relation) = {
    addressMap.get(relation.address) match {
      case Some(v) => v
      case None => registorRelation(relation)
    }
  }

}

object LogoCatalog {
  lazy val _catalog = new LogoCatalog

  def getCatalog() = _catalog
}

