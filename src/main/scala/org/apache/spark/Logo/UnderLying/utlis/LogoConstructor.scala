package org.apache.spark.Logo.UnderLying.utlis

import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{LogoCatalog, Relation, RelationSchema, RelationWithP}
import org.apache.spark.Logo.Plan.{LogoRDDReference, PatternLogoRDDReference}

class LogoConstructor(nodeOrder:Seq[Int], edges:Map[Int,Seq[Relation]], defaultP:Int) {


  lazy val catalog = LogoCatalog.getCatalog()
  lazy val relationSchema = RelationSchema.getRelationSchema()

  def initEdge():SubPattern = {
    val baseLogoRelation = edges(nodeOrder(1))(0).toRelationWithP(Seq(defaultP, defaultP))
    val baseLogo = catalog.retrieveLogo(baseLogoRelation).get
    new SubPattern(EmptySubPattern(), baseLogo, baseLogoRelation.attributes.zipWithIndex.toMap)
  }

  def initSampledEdge(k:Long):SubPattern = ???



  def constructSampleLogo():PatternLogoRDDReference = {
    val subPattern1 = initEdge()
    val res = nodeOrder.drop(2)
      .map(relationSchema.getAttribute)
      .foldLeft(subPattern1)((subPattern,attr) =>
        subPattern.build(attr,
          edges(relationSchema
            .getAttributeId(attr))
            .map(_.toRelationWithP(Seq(defaultP,defaultP)))))

    print(res)
    res.logo
  }

}


class SubPattern(val prevPattern:SubPattern, val logo:PatternLogoRDDReference, val globalAttributeToLocalMapping:Map[String,Int]){

  lazy val catalog = LogoCatalog.getCatalog()
  var stringCommand:String = ""

  def generateIDForNewAttr(newAttr:String):Int = {
    globalAttributeToLocalMapping.values.max + 1
  }

  def build(newAttr:String, newRelations:Seq[RelationWithP]):SubPattern = {
    val newLocalID = generateIDForNewAttr(newAttr)
    val newGlobalAttributeToLocalMapping = globalAttributeToLocalMapping + ((newAttr, newLocalID))
    val newLogos = newRelations.map(catalog.retrieveLogo)

    def build1() = {
      val newSubLogo1 = newLogos(0).get
      val relation1 = newRelations(0)

      stringCommand = s"logo.build(${relation1.name}.toWithSeqKeyMapping(${relation1.attributes.map(newGlobalAttributeToLocalMapping.get).map(_.get)})"

      val newLogo = logo.build(newSubLogo1.toWithSeqKeyMapping(relation1.attributes.map(newGlobalAttributeToLocalMapping.get).map(_.get)))
        new SubPattern(this, newLogo, newGlobalAttributeToLocalMapping)
    }

    def build2() = {
      val newSubLogo1 = newLogos(0).get
      val relation1 = newRelations(0)

      val newSubLogo2 = newLogos(1).get
      val relation2 = newRelations(1)

      stringCommand =
        s"""
           |logo.build(
           |        ${newSubLogo1}.toWithSeqKeyMapping(${relation1.attributes.map(newGlobalAttributeToLocalMapping.get).map(_.get)}),
           |        ${newSubLogo2}.toWithSeqKeyMapping(${relation2.attributes.map(newGlobalAttributeToLocalMapping.get).map(_.get)})
           |      )
         """.stripMargin

      val newLogo = logo.build(
        newSubLogo1.toWithSeqKeyMapping(relation1.attributes.map(newGlobalAttributeToLocalMapping.get).map(_.get)),
        newSubLogo2.toWithSeqKeyMapping(relation2.attributes.map(newGlobalAttributeToLocalMapping.get).map(_.get))
      )

      new SubPattern(this, newLogo, newGlobalAttributeToLocalMapping)
    }

    def build3() = {
      val newSubLogo1 = newLogos(0).get
      val relation1 = newRelations(0)

      val newSubLogo2 = newLogos(1).get
      val relation2 = newRelations(1)

      val newSubLogo3 = newLogos(2).get
      val relation3 = newRelations(2)

      stringCommand =
        s"""
           |logo.build(
           |        ${newSubLogo1}.toWithSeqKeyMapping(${relation1.attributes.map(newGlobalAttributeToLocalMapping.get).map(_.get)}),
           |        ${newSubLogo2}.toWithSeqKeyMapping(${relation2.attributes.map(newGlobalAttributeToLocalMapping.get).map(_.get)}),
           |        ${newSubLogo3}.toWithSeqKeyMapping(${relation3.attributes.map(newGlobalAttributeToLocalMapping.get).map(_.get)}
           |      )
         """.stripMargin

      val newLogo = logo.build(
        newSubLogo1.toWithSeqKeyMapping(relation1.attributes.map(newGlobalAttributeToLocalMapping.get).map(_.get)),
        newSubLogo2.toWithSeqKeyMapping(relation2.attributes.map(newGlobalAttributeToLocalMapping.get).map(_.get)),
        newSubLogo3.toWithSeqKeyMapping(relation3.attributes.map(newGlobalAttributeToLocalMapping.get).map(_.get))
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

case class EmptySubPattern() extends SubPattern(null,null,null){
  override def toString: String = {
    s"""
       |SubPattern
     """.stripMargin
  }
}

