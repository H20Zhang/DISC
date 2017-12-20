package org.apache.spark.Logo.Physical.Builder

import org.apache.spark.Logo.Physical.Joiner.{FetchJoinRDD, SubTask}
import org.apache.spark.Logo.Physical.dataStructure.{LogoBlockRef, _}
import org.apache.spark.Logo.Physical.utlis.ListGenerator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


/**
  * representing the steps needed to construct an "LOGO"
  * @param logoSteps steps used to construct an "LOGO"
  */
class LogoBuildScript(logoSteps:List[LogoBuildScriptStep]) {}

trait LogoBuildScriptStep

/**
  *
  * @param lRDDID lRDD's ID in rdds
  * @param lRDDSlot lRDD's slot to be snaped with rRDD's slot
  * @param rRDDID rRDD's ID in rdds
  * @param rRDDSlot rRDD's slot to be snaped with lRDD's slot
  */
case class SnapPoint(lRDDID:Int, lRDDSlot:Int, rRDDID:Int, rRDDSlot:Int)


/**
  * define the joint between two blocks
  * @param coreBlockID the Id of core
  * @param leafBlockID the Id of leaf
  * @param coreJoints the joints of the coreBlock
  * @param leafJoints the joints of the leafBlock
  */
case class BlockBlockJoints(coreBlockID:Int, leafBlockID:Int, coreJoints:Seq[Int], leafJoints:Seq[Int])


//TODO should relocate composite schema generate part to logical part.
/**
  * Represent one step in building an "LOGO" by specifying how logoRDD are snapped into each other
  *
  * @param logoRDDRefs logoRDDs used to build the new logo
  * @param snapPoints how logo's are snapped to each other
  * @param handler how to handle the LogoBlock after the LogoBlocks are already snapped together by FetchJoin
  */
case class LogoBuildPhyiscalStep(logoRDDRefs:List[LogoRDD], snapPoints:List[SnapPoint], handler: (Seq[LogoBlockRef],CompositeLogoSchema) => LogoBlockRef, name:String="") extends LogoBuildScriptStep{

  lazy val schemas = logoRDDRefs.map(_.schema)
  lazy val rdds = logoRDDRefs.map(_.logoRDD)
  lazy val intersectionMapping = generateIntersectionMapping()
  lazy val compositeSchema = generateCompositeSchema()
  lazy val subtasks = generateSubTasks()


  //from the snapPoints generate the IntersectionMapping(define which two nodes mapping to the same new nodes)
  def generateIntersectionMapping():List[Map[Int,Int]] = {
//    val totalRDDNum = snapPoints.flatMap(f => Iterator(f.lRDDID,f.rRDDID)).max
    val partialMappingTemp = new Array[Map[Int,Int]](logoRDDRefs.size).map(f => Map[Int,Int]())
    snapPoints.foldLeft(0){(curIndex,point) =>
      var curIndexTemp = curIndex
      val lRDDMap = partialMappingTemp(point.lRDDID)
      val rRDDMap = partialMappingTemp(point.rRDDID)
      val lPrevMapping = lRDDMap.getOrElse(point.lRDDSlot,-1)
      val rPrevMapping = rRDDMap.getOrElse(point.rRDDSlot,-1)

      if (lPrevMapping != -1 || rPrevMapping != -1){
        if (lPrevMapping != -1){
          val index = lPrevMapping
          partialMappingTemp.update(point.rRDDID, rRDDMap + ((point.rRDDSlot,index)))
        }

        if (rPrevMapping != -1){
          val index = rPrevMapping
          partialMappingTemp.update(point.lRDDID, lRDDMap + ((point.lRDDSlot,index)))
        }
      } else {
        partialMappingTemp.update(point.lRDDID, lRDDMap + ((point.lRDDSlot,curIndexTemp)))
        partialMappingTemp.update(point.rRDDID, rRDDMap + ((point.rRDDSlot,curIndexTemp)))
        curIndexTemp += 1
      }

      curIndexTemp}

    partialMappingTemp.toList
  }

  //generate the composite schema
  def generateCompositeSchema() = {
      CompositeLogoSchema(schemas,intersectionMapping.map(f => KeyMapping(f)),name)
  }

  //generate the subTasks
  def generateSubTasks() = {
    val newSchemaSlots = compositeSchema.slotSize
    val newSchemaSubTasks = ListGenerator.cartersianSizeList(newSchemaSlots)

    val oldIndexs = newSchemaSubTasks.map(f => compositeSchema.newKeyToOldIndex(f))
    val subtasks = oldIndexs.map(SubTask(_,rdds,compositeSchema))

    subtasks
  }

  //generate the FetchJoinRDD
  def performFetchJoin(sc:SparkContext) = {
    new FetchJoinRDD(sc,subtasks,compositeSchema, handler,rdds)
  }

  //TODO finish this
  //generate the logoBlockRef and add it to catalog
  def generateLogoRef(sc:SparkContext) = ???
}


