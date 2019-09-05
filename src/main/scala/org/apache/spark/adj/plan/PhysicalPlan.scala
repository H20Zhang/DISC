package org.apache.spark.adj.plan

import org.apache.spark.adj.database.Catalog.{AttributeID, DataType}
import org.apache.spark.adj.database.{Catalog, Relation, RelationSchema}
import org.apache.spark.adj.execution.hcube.{HCube, HCubePlan, TupleHCubeBlock}
import org.apache.spark.adj.execution.misc.DataLoader
import org.apache.spark.adj.execution.subtask.{
  AttributeOrderInfo,
  LeapFrogJoinSubTask
}
import org.apache.spark.rdd.RDD

//physical plan is the plan that describe the distributed execution process
trait PhysicalPlan {
  def execute(): Relation
  def count(): Long
  def getChildren(): Seq[PhysicalPlan]
  val db = Catalog.defaultCatalog()
}

//TODO: finish it
case class ADJLeapJoinExec(children: Seq[PhysicalPlan],
                           share: Map[AttributeID, Int],
                           attrOrder: Seq[AttributeID],
                           tasksNum: Int = 4)
    extends PhysicalPlan {

  override def execute(): Relation = ???

  override def getChildren(): Seq[PhysicalPlan] = children

  override def count(): Long = ???
}

case class FactorizedLeapJoinExec(children: Seq[PhysicalPlan],
                                  share: Map[AttributeID, Int],
                                  attrOrder: Seq[AttributeID],
                                  tasksNum: Int = 4)
    extends PhysicalPlan {

  override def execute(): Relation = ???

  override def getChildren(): Seq[PhysicalPlan] = children

  override def count(): Long = ???
}

case class HCubeLeapJoinExec(children: Seq[PhysicalPlan],
                             share: Map[AttributeID, Int],
                             attrOrder: Seq[AttributeID],
                             tasksNum: Int = 4)
    extends PhysicalPlan {

  override def execute(): Relation = {
    val relations = getChildren().map(_.execute()).toArray
    val hcubePlan = HCubePlan(relations, share)
    val attrOrderInfo = AttributeOrderInfo(attrOrder.toArray)
    val hcube = new HCube(hcubePlan, attrOrderInfo)
    val rdd = hcube
      .genHCubeRDD()
      .flatMap { task =>
        val orderInfo = task.info.asInstanceOf[AttributeOrderInfo]
        val subJoinTask = new LeapFrogJoinSubTask(
          task.shareVector,
          task.blocks.map(_.asInstanceOf[TupleHCubeBlock]),
          orderInfo
        )
        val iterator = subJoinTask.execute()
        iterator
      }

    val catalog = Catalog.defaultCatalog()
    val schema = RelationSchema(
      s"R${catalog.nextRelationID()}",
      attrOrder.map(attrId => catalog.getAttribute(attrId))
    )
    Relation(schema, rdd)
  }

  override def getChildren(): Seq[PhysicalPlan] = children

  override def count(): Long = {

    //TODO: test parallel support
//    val forkJoinPool = new ForkJoinPool(4)
//    val tasks = getChildren().par
//    tasks.tasksupport = new ForkJoinTaskSupport(forkJoinPool)

    val relations = getChildren().map(_.execute()).toArray
    val hcubePlan = HCubePlan(relations, share)
    val attrOrderInfo = AttributeOrderInfo(attrOrder.toArray)
    val hcube = new HCube(hcubePlan, attrOrderInfo)
    val num = hcube
      .genHCubeRDD()
      .map { task =>
        val orderInfo = task.info.asInstanceOf[AttributeOrderInfo]
        val subJoinTask = new LeapFrogJoinSubTask(
          task.shareVector,
          task.blocks.map(_.asInstanceOf[TupleHCubeBlock]),
          orderInfo
        )
        val iterator = subJoinTask.execute()
        iterator.longSize()
//        1
      }
      .sum()
    num.toLong
  }
}

case class DiskScanExec(schema: RelationSchema, dataAddress: String)
    extends PhysicalPlan {
  override def execute(): Relation = {
    val loader = new DataLoader()
    Relation(schema, loader.csv(dataAddress))
  }

  override def getChildren(): Seq[PhysicalPlan] = Seq()

  override def count(): Long = {
    execute().rdd.count()
  }
}

case class InMemoryScanExec(schema: RelationSchema,
                            content: RDD[Array[DataType]])
    extends PhysicalPlan {
  override def execute(): Relation = {
    Relation(schema, content)
  }

  override def getChildren(): Seq[PhysicalPlan] = Seq()

  override def count(): Long = {
    execute().rdd.count()
  }
}
