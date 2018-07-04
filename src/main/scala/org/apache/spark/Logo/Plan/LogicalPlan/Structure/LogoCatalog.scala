package org.apache.spark.Logo.Plan.LogicalPlan.Structure

import org.apache.spark.Logo.Plan.{LogoRDDReference, PatternLogoRDDReference}
import org.apache.spark.Logo.UnderLying.Loader.{EdgeLoader, EdgePatternLoader}
import org.apache.spark.Logo.UnderLying.dataStructure.LogoRDD
import org.apache.spark.Logo.UnderLying.utlis.EdgeLoader

import scala.collection.mutable

/**
  * logoCatlog used to associate a name with a RDD[LogoRDDReference],
  * LogoRDDReference records the RDD[LogoBlock] and its according schema
  */
class LogoCatalog {
  val rddMap: mutable.Map[RelationWithP, PatternLogoRDDReference] = mutable.Map()

  def registorRelationFromAdress(relation: RelationWithP, data:String):PatternLogoRDDReference = {
    val rawEdge = new EdgeLoader(data) rawEdgeRDD

    val edgeLoader = new EdgePatternLoader(rawEdge, Seq(relation.p(0), relation.p(1)))
    val res = edgeLoader.edgeLogoRDDReference

    linkRelationWithLogo(relation, res)

    res
  }

  def linkRelationWithLogo(relation:RelationWithP, logo:PatternLogoRDDReference) = {
    rddMap.put(relation, logo)
  }

  def retrieveLogo(relation:RelationWithP) = rddMap.get(relation)


//  def putLogo(name: String, rdd: LogoRDDReference): Unit = {
//    rddMap += ((name, rdd))
//  }
//
//  def getLogo(name: String) = {
//    rddMap(name)
//  }
//
//  def removeLogo(name: String): Unit = {
//    rddMap -= name
//  }

}

object LogoCatalog {
  lazy val _catalog = new LogoCatalog

  def getCatalog() = _catalog
}

