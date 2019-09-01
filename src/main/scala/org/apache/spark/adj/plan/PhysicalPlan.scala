package org.apache.spark.adj.plan

import org.apache.spark.adj.execution.hcube.{HCube, HCubePlan, TupleHCubeBlock}
import org.apache.spark.adj.database.Catalog.{
  Attribute,
  AttributeID,
  DataType,
  RelationID
}
import org.apache.spark.adj.database.{Catalog, Relation, RelationSchema}
import org.apache.spark.adj.execution.misc.DataLoader
import org.apache.spark.adj.optimization.ShareComputer
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.parallel.{
  ForkJoinTaskSupport,
  ParSeq,
  ThreadPoolTaskSupport
}
import scala.concurrent.forkjoin.ForkJoinPool

//physical plan is the plan that describe the distributed execution process
trait PhysicalPlan {
  def execute(): Relation
  def count(): Long
  def getChildren(): Seq[PhysicalPlan]
  val db = Catalog.defaultCatalog()
}

case class HCubeLeapJoinExec(children: Seq[PhysicalPlan],
                             share: Map[AttributeID, Int],
                             attrOrder: Array[AttributeID],
                             tasksNum: Int = 4)
    extends PhysicalPlan {

  override def execute(): Relation = {
    throw new NotImplementedError()
  }

  override def getChildren(): Seq[PhysicalPlan] = children

  override def count(): Long = {

    //TODO: test parallel support
    val forkJoinPool = new ForkJoinPool(4)
    val tasks = getChildren().par
    tasks.tasksupport = new ForkJoinTaskSupport(forkJoinPool)

    val relations = getChildren().map(_.execute()).toArray
    val hcubePlan = HCubePlan(relations, share)
    val attrOrderInfo = AttributeOrderInfo(attrOrder)
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
        iterator.size
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
    execute().content.count()
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
    execute().content.count()
  }
}
