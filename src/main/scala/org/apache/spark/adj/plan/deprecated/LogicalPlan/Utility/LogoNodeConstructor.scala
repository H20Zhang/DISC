package org.apache.spark.adj.plan.deprecated.LogicalPlan.Utility

import org.apache.log4j.LogManager
import org.apache.spark.adj.plan.deprecated.LogicalPlan.Structure._
import org.apache.spark.adj.plan.deprecated.PhysicalPlan.Logo
import org.apache.spark.adj.execution.rdd.loader.EdgeLoader
import org.apache.spark.rdd.RDD

import scala.util.Random

class LogoNodeConstructor(attrOrder:Seq[Int], edges:Map[Int,Seq[Relation]]){

  val log = LogManager.getLogger(this.getClass)

  val conf = Configuration.getConfiguration()
  val defaultSampleP = conf.defaultSampleP

  lazy val catalog = LogoCatalog.getCatalog()
  lazy val relationSchema = RelationSchema.getRelationSchema()

  def makeEdge(inputEdge:RDD[(Array[Int],Int)],hNumber: (Int, Int), attrIDTuple:(Int,Int)): SubPattern = {
    val edgeLogo = new EdgeLoader(inputEdge, Seq(hNumber._1, hNumber._2)) edgeLogoRDDReference
    val pattern = SubPattern(edgeLogo, attrIDTuple)
    pattern
  }

  def initEdge(p:Map[Int,Int]):SubPattern = {
    val baseRelation = edges(attrOrder(1))(0)
    val baseLogoRelation = baseRelation.toRelationWithP(p)
    val baseLogo = catalog.retrieveOrRegisterRelationWithP(baseLogoRelation)
    new SubPattern(EmptySubPattern(), baseLogo, baseLogoRelation.attr.zipWithIndex.toMap)
  }

  def initSampledEdge(k:Long):SubPattern = {

    val baseLogoRelation = edges(attrOrder(1))(0)
    val baseSamplingLogoRelation = baseLogoRelation.toRelationWithP(Seq(defaultSampleP, defaultSampleP))

    log.info(s"start generating sampled relations:${baseSamplingLogoRelation}")

    val baseSampleLogo = catalog.getSampledRelationWithP(baseSamplingLogoRelation,k)
    val ratio = k.toDouble / baseLogoRelation.cardinality

    val pattern = new SubPattern(EmptySubPattern(), baseSampleLogo, baseSamplingLogoRelation.attr.zipWithIndex.toMap)
    val sampledPattern = SampledSubPattern(pattern, ratio)
    sampledPattern
  }

  def initSampledPatternFromAttrTuple(k:Long, attrIDTuples:(Int,Int)):SubPattern = {
    val pattern = constructSampleLogoWithEdgeLimit((k).toLong)
    val patternLogo = pattern.logo
    val globalToLocalMap = pattern.globalAttributeToLocalMapping
    val localIDTuples = (globalToLocalMap(relationSchema.getAttribute(attrIDTuples._1)),globalToLocalMap(relationSchema.getAttribute(attrIDTuples._2)))
    val newEdge = patternLogo.rdd().map(f => (f(localIDTuples._1),f(localIDTuples._2)))

    val base = k.toInt
    val count = patternLogo.size()
    val ratio = base/count.toDouble

    if (ratio > 1){
      println("ratio should less than 1")
    }

    val sampledNewEdge = newEdge.mapPartitions{f =>
      val random = Random
      random.setSeed(System.nanoTime())
      f.filter(p => random.nextInt(base) < base*ratio)
    }.map(f => (Array(f._1, f._2), 1))

    val initPattern = makeEdge(sampledNewEdge, (defaultSampleP, defaultSampleP), attrIDTuples)
    initPattern
  }


  def constructWithInitPattern(remainAttrOrder:Seq[Int], pattern:SubPattern, p:Map[Int,Int]):SubPattern = {
    val res = remainAttrOrder
      .map(relationSchema.getAttribute)
      .foldLeft(pattern)((subPattern,attr) =>
        subPattern.build(attr,
          edges(relationSchema
            .getAttributeId(attr))
            .map(_.toRelationWithP(p))))

//    print(res)
    res
  }

  def constructPattern(p:Map[Int,Int]):SubPattern = {
    val sampledEdge = initEdge(p)
    val remainAttrOrder = attrOrder.diff(sampledEdge.allAttributeIDs)
    constructWithInitPattern(remainAttrOrder, sampledEdge, p)
  }

  def constructSampleLogoWithEdgeLimit(k:Long):SubPattern = {

    log.info(s"start generating sampled pattern:${edges.values.flatten} with order:${attrOrder.map(relationSchema.getAttribute)}")
    val sampledEdge = initSampledEdge(k)
    val remainAttrOrder = attrOrder.diff(sampledEdge.allAttributeIDs)
    val p = attrOrder.map((_,defaultSampleP)).toMap
    constructWithInitPattern(remainAttrOrder, sampledEdge, p)
  }

  def constructSampleLogoWithInitPattern(remainAttrOrder:Seq[Int], pattern:SubPattern):SubPattern = {
    log.info(s"start generating sampled pattern:${edges.values.flatten} with order:${attrOrder.map(relationSchema.getAttribute)} with initPattern:${pattern}")
    val p = attrOrder.map((_,defaultSampleP)).toMap
    constructWithInitPattern(remainAttrOrder, pattern, p)
  }



}


class SubPattern(val prevPattern:SubPattern, @transient val logo:Logo, val globalAttributeToLocalMapping:Map[String,Int]) extends Serializable {

  lazy val catalog = LogoCatalog.getCatalog()
  var stringCommand:String = ""
  lazy val relationSchema = RelationSchema.getRelationSchema()



  def attrIds() = {
    globalAttributeToLocalMapping.keys.map(relationSchema.getAttributeId).toSeq
  }

  def globalAttributeIDToLocalID(globalattrID:Int) = {
    globalAttributeToLocalMapping(relationSchema.getAttribute(globalattrID))
  }

  def allAttributeIDs:Seq[Int] = {
    globalAttributeToLocalMapping.keys.toSeq.map(relationSchema.getAttributeId)
  }

  def generateIDForNewAttr(newAttr:String):Int = {
    globalAttributeToLocalMapping.values.max + 1
  }

  def build(subPattern: SubPattern):SubPattern = {
    val rhsAttrIds = subPattern.attrIds()
    val lhsAttrIds = attrIds()
    val remainAttrIds = rhsAttrIds.diff(lhsAttrIds)
    val remainAttrs = remainAttrIds.map(relationSchema.getAttribute)
    var newGlobalAttributeToLocalMapping = globalAttributeToLocalMapping

    remainAttrs.foreach{f =>
      val newLocalID = generateIDForNewAttr(f)
      newGlobalAttributeToLocalMapping = newGlobalAttributeToLocalMapping + ((f, newLocalID))
    }

    val keyMapping = subPattern.globalAttributeToLocalMapping.toSeq.sortBy(_._2).map(_._1).map(newGlobalAttributeToLocalMapping)

    val newLogo = logo.build(subPattern.logo.toWithSeqKeyMapping(keyMapping))
    new SubPattern(this, newLogo, newGlobalAttributeToLocalMapping)

  }

  def build(newAttr:String, newRelations:Seq[RelationWithP]):SubPattern = {
    val newLocalID = generateIDForNewAttr(newAttr)
    val newGlobalAttributeToLocalMapping = globalAttributeToLocalMapping + ((newAttr, newLocalID))

//    println(newRelations)
//    println(catalog.rddMap)

    val newLogos = newRelations.map(catalog.retrieveOrRegisterRelationWithP)

    def build1() = {
      val newSubLogo1 = newLogos(0)
      val relation1 = newRelations(0)

      stringCommand = s"logo.build(${relation1.name}.toWithSeqKeyMapping(${relation1.attr.map(newGlobalAttributeToLocalMapping.get).map(_.get)})"

      val newLogo = logo.build(newSubLogo1.toWithSeqKeyMapping(relation1.attr.map(newGlobalAttributeToLocalMapping.get).map(_.get)))
        new SubPattern(this, newLogo, newGlobalAttributeToLocalMapping)
    }

    def build2() = {
      val newSubLogo1 = newLogos(0)
      val relation1 = newRelations(0)

      val newSubLogo2 = newLogos(1)
      val relation2 = newRelations(1)

      stringCommand =
        s"""
           |logo.build(
           |        ${relation1.name}.toWithSeqKeyMapping(${relation1.attr.map(newGlobalAttributeToLocalMapping.get).map(_.get)}),
           |        ${relation2.name}.toWithSeqKeyMapping(${relation2.attr.map(newGlobalAttributeToLocalMapping.get).map(_.get)})
           |      )
         """.stripMargin

      val newLogo = logo.build(
        newSubLogo1.toWithSeqKeyMapping(relation1.attr.map(newGlobalAttributeToLocalMapping.get).map(_.get)),
        newSubLogo2.toWithSeqKeyMapping(relation2.attr.map(newGlobalAttributeToLocalMapping.get).map(_.get))
      )

      new SubPattern(this, newLogo, newGlobalAttributeToLocalMapping)
    }

    def build3() = {
      val newSubLogo1 = newLogos(0)
      val relation1 = newRelations(0)

      val newSubLogo2 = newLogos(1)
      val relation2 = newRelations(1)

      val newSubLogo3 = newLogos(2)
      val relation3 = newRelations(2)

      stringCommand =
        s"""
           |logo.build(
           |        ${relation1.name}.toWithSeqKeyMapping(${relation1.attr.map(newGlobalAttributeToLocalMapping.get).map(_.get)}),
           |        ${relation2.name}.toWithSeqKeyMapping(${relation2.attr.map(newGlobalAttributeToLocalMapping.get).map(_.get)}),
           |        ${relation3.name}.toWithSeqKeyMapping(${relation3.attr.map(newGlobalAttributeToLocalMapping.get).map(_.get)}
           |      )
         """.stripMargin

      val newLogo = logo.build(
        newSubLogo1.toWithSeqKeyMapping(relation1.attr.map(newGlobalAttributeToLocalMapping.get).map(_.get)),
        newSubLogo2.toWithSeqKeyMapping(relation2.attr.map(newGlobalAttributeToLocalMapping.get).map(_.get)),
        newSubLogo3.toWithSeqKeyMapping(relation3.attr.map(newGlobalAttributeToLocalMapping.get).map(_.get))
      )

      new SubPattern(this, newLogo, newGlobalAttributeToLocalMapping)
    }

    newRelations.size match {
      case 1 => build1()
      case 2 => build2()
      case 3 => build3()
    }
  }

  override def toString: String = {
    s"""
       |$prevPattern
       |$stringCommand
     """.stripMargin
  }

}

object SubPattern{
  def apply(edgelogo:Logo, attrIDTuple:(Int, Int)) = {
    val prevPattern = EmptySubPattern()
    val relationSchema = RelationSchema.getRelationSchema()
    val globalToLocalAttrMapping = Seq(attrIDTuple._1, attrIDTuple._2).map(relationSchema.getAttribute).zipWithIndex.toMap
    val pattern = new SubPattern(prevPattern, edgelogo, globalToLocalAttrMapping)
    pattern
  }
}

class SampledSubPattern(val ratio:Double,
                        prevPattern:SubPattern,
                        logo:Logo,
                        globalAttributeToLocalMapping:Map[String,Int]) extends SubPattern(prevPattern, logo, globalAttributeToLocalMapping){

  override def build(newAttr: String, newRelations: Seq[RelationWithP]): SubPattern = {
    val pattern = super.build(newAttr, newRelations)
    SampledSubPattern(pattern, ratio)
  }
}

object SampledSubPattern{
  def apply(subPattern: SubPattern, ratio:Double) = new SampledSubPattern(ratio, subPattern.prevPattern, subPattern.logo, subPattern.globalAttributeToLocalMapping)
}

case class EmptySubPattern() extends SubPattern(null,null,null){
  override def toString: String = {
    s"""
       |SubPattern
     """.stripMargin
  }
}

