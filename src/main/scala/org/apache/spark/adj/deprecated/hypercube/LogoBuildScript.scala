package org.apache.spark.adj.deprecated.hypercube

import org.apache.spark.adj.deprecated.execution.rdd.{LogoBlockRef, _}
import org.apache.spark.adj.utlis.ListGenerator
import org.apache.spark.SparkContext


/**
  * representing the steps needed to construct an "LOGO"
  *
  * @param logoSteps steps used to construct an "LOGO"
  */
class LogoBuildScript(logoSteps: List[LogoPhysicalPlan]) {}

trait LogoPhysicalPlan

/**
  *
  * @param lRDDID   lRDD's ID in rdds
  * @param lRDDSlot lRDD's slot to be snaped with rRDD's slot
  * @param rRDDID   rRDD's ID in rdds
  * @param rRDDSlot rRDD's slot to be snaped with lRDD's slot
  */
case class SnapPoint(lRDDID: Int, lRDDSlot: Int, rRDDID: Int, rRDDSlot: Int)


/**
  * define the joint between two blocks
  *
  * @param coreBlockID the Id of core
  * @param leafBlockID the Id of leaf
  * @param coreJoints  the joints of the coreBlock
  * @param leafJoints  the joints of the leafBlock
  */
case class BlockBlockJoints(coreBlockID: Int, leafBlockID: Int, coreJoints: Set[Int], leafJoints: Set[Int])


//TODO should relocate composite schema generate part to logical part.
/**
  * Represent one step in building an "LOGO" by specifying how logoRDD are snapped into each other
  *
  * @param logoRDDRefs logoRDDs used to build the new logo
  * @param handler     how to handle the LogoBlock after the LogoBlocks are already snapped together by FetchJoin
  */
case class LogoBuildPhyiscalStep(logoRDDRefs: Seq[LogoRDD], schema: CompositeLogoSchema, handler: (Seq[LogoBlockRef], CompositeLogoSchema, Int) => LogoBlockRef, name: String = "") extends LogoPhysicalPlan {

  lazy val schemas = logoRDDRefs.map(_.schema)
  lazy val rdds = logoRDDRefs.map(_.logoRDD)
  lazy val compositeSchema = schema
  lazy val subtasks = generateSubTasks()

  //generate the subTasks
  def generateSubTasks() = {
    val newSchemaSlots = compositeSchema.slotSize
    val newSchemaSubTasks = ListGenerator.cartersianSizeList(newSchemaSlots)

    val oldIndexs = newSchemaSubTasks.map(f => compositeSchema.newKeyToOldIndex(f))
    val subtasks = oldIndexs.map(SubTask(_, rdds, compositeSchema))

    subtasks
  }

  //generate the FetchJoinRDD
  def performFetchJoin(sc: SparkContext) = {
    new FetchJoinRDD(sc, subtasks, compositeSchema, handler, rdds)
  }

}


