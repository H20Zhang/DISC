package org.apache.spark.adj.plan

import org.apache.spark.adj.database.Catalog.{AttributeID, DataType}
import org.apache.spark.adj.database.{Catalog, Relation, RelationSchema}
import org.apache.spark.adj.execution.hcube.pull.{
  HCubePlan,
  PartitionedRelation,
  PullHCube
}
import org.apache.spark.adj.execution.hcube.{TrieHCubeBlock, TupleHCubeBlock}
import org.apache.spark.adj.execution.hcube.push.PushHCube
import org.apache.spark.adj.execution.hcube.utils.TriePreConstructor
import org.apache.spark.adj.execution.misc.DataLoader
import org.apache.spark.adj.execution.subtask.{
  AttributeOrderInfo,
  CachedLeapFrogAttributeOrderInfo,
  FactorizedAttributeOrderInfo,
  FactorizedLeapFrogJoinSubTask,
  LeapFrogJoinSubTask,
  SubTask,
  SubTaskFactory,
  TaskInfo,
  TrieConstructedAttributeOrderInfo
}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

//physical adj.plan is the adj.plan that describe the distributed execution process
trait PhysicalPlan {
  def execute(): Relation
  def count(): Long
  def commOnly(): Long
  def getChildren(): Seq[PhysicalPlan]
  val catalog = Catalog.defaultCatalog()
  val outputSchema: RelationSchema
  lazy val outputRelationString =
    s"${outputSchema.name}${outputSchema.attrs.mkString("(", ", ", ")")}"
  def prettyString(): String = {

    if (getChildren().nonEmpty) {
      val childrenString =
        getChildren()
          .map(child => s"${child.prettyString()}\n")
          .reduce(_ + _)
          .dropRight(1)
          .split("\n")
          .map(str => s"\t${str}\n")
          .reduce(_ + _)
          .dropRight(1)

      s"-${selfString()}->${outputRelationString}\n${childrenString}"
    } else {
      s"-${selfString()}->${outputRelationString}"
    }
  }

  def selfString(): String = {
    s"unknown"
  }
}

abstract class JoinExec(schema: RelationSchema,
                        @transient children: Seq[PhysicalPlan])
    extends PhysicalPlan {
  override val outputSchema: RelationSchema = schema

  def getChildren(): Seq[PhysicalPlan] = {
    children
  }
}

abstract class AbstractHCubeJoinExec(schema: RelationSchema,
                                     @transient children: Seq[PhysicalPlan],
                                     share: Map[AttributeID, Int],
                                     info: TaskInfo)
    extends JoinExec(schema, children)
    with Serializable {

  @transient val relations = getChildren().map(_.execute()).toArray
  @transient val hcubePlan = HCubePlan(relations, share)
  @transient val subTaskRDD = genSubTaskRDD()

  def genSubTaskRDD(): RDD[SubTask]

  def execute(): Relation = {
    val rdd = subTaskRDD
      .flatMap { task =>
        val subJoinTask =
          SubTaskFactory.genSubTask(task.shareVector, task.blocks, task.info)

        val iterator = subJoinTask.execute()
        iterator
      }

//    schema.setContent(rdd)
    catalog.setContent(schema, rdd)
    Relation(schema, rdd)
  }

  def count() = {
    val num = subTaskRDD
      .map { task =>
        val subJoinTask =
          SubTaskFactory.genSubTask(task.shareVector, task.blocks, task.info)

        val iterator =
          subJoinTask.execute()
        iterator.longSize

      }
      .sum()
      .toLong

    num
  }

  def commOnly(): Long = {
    val num = subTaskRDD
      .map { task =>
        1
      }
      .sum()
    num.toLong
  }

}

abstract class AbstractMergedPullHCubeJoinExec(
  schema: RelationSchema,
  @transient children: Seq[PhysicalPlan],
  share: Map[AttributeID, Int],
  info: TaskInfo
) extends AbstractHCubeJoinExec(schema, children, share, info) {

  def preprocessHCube(f: PartitionedRelation): PartitionedRelation = {
    val info_ = info
    val rdd = f.partitionedRDD
    val partitioner = f.partitioner

    val preprocessedRDD = rdd.map { block =>
      val tupleHCubeBlock = block.asInstanceOf[TupleHCubeBlock]
      val schema = tupleHCubeBlock.schema
      val content = tupleHCubeBlock.content
      val shareVector = tupleHCubeBlock.shareVector
      var outputBlock = block

      info_ match {
        case s: TrieConstructedAttributeOrderInfo => {
          val attrOrders = s.attrOrder
          val triePreConstructor =
            new TriePreConstructor(attrOrders, schema, content)
          val trie = triePreConstructor.construct()
          outputBlock = TrieHCubeBlock(schema, shareVector, trie)
        }
        case _ => throw new Exception(s"not supported taskinfo:${info_}")
      }

      outputBlock
    }

    preprocessedRDD.cache()
    preprocessedRDD.count()

    PartitionedRelation(preprocessedRDD, partitioner)
  }

  override def genSubTaskRDD(): RDD[SubTask] = {
    val hcube = new PullHCube(hcubePlan, info)
    hcube.genPullHCubeRDD(preprocessHCube)
  }
}

abstract class AbstractPullHCubeJoinExec(schema: RelationSchema,
                                         children: Seq[PhysicalPlan],
                                         share: Map[AttributeID, Int],
                                         info: TaskInfo)
    extends AbstractHCubeJoinExec(schema, children, share, info) {

  override def genSubTaskRDD(): RDD[SubTask] = {
    val hcube = new PullHCube(hcubePlan, info)
    hcube.genHCubeRDD()
  }
}

abstract class AbstractPushHCubeJoinExec(schema: RelationSchema,
                                         children: Seq[PhysicalPlan],
                                         share: Map[AttributeID, Int],
                                         info: TaskInfo)
    extends AbstractHCubeJoinExec(schema, children, share, info) {

  override def genSubTaskRDD(): RDD[SubTask] = {
    val hcube = new PushHCube(hcubePlan, info)
    hcube.genHCubeRDD()
  }
}

case class PullFactorizedLeapJoinExec(schema: RelationSchema,
                                      children: Seq[PhysicalPlan],
                                      share: Map[AttributeID, Int],
                                      attrOrder: Seq[AttributeID],
                                      corePos: Int,
                                      tasksNum: Int = 4)
    extends AbstractPullHCubeJoinExec(
      schema,
      children,
      share,
      FactorizedAttributeOrderInfo(attrOrder.toArray, corePos)
    )

case class PullHCubeLeapJoinExec(schema: RelationSchema,
                                 children: Seq[PhysicalPlan],
                                 share: Map[AttributeID, Int],
                                 attrOrder: Seq[AttributeID],
                                 tasksNum: Int)
    extends AbstractPullHCubeJoinExec(
      schema,
      children,
      share,
      AttributeOrderInfo(attrOrder.toArray)
    )

case class PushHCubeLeapJoinExec(schema: RelationSchema,
                                 children: Seq[PhysicalPlan],
                                 share: Map[AttributeID, Int],
                                 attrOrder: Seq[AttributeID],
                                 tasksNum: Int)
    extends AbstractPushHCubeJoinExec(
      schema,
      children,
      share,
      AttributeOrderInfo(attrOrder.toArray)
    )

case class PushHCubeCachedLeapJoinExec(schema: RelationSchema,
                                       children: Seq[PhysicalPlan],
                                       share: Map[AttributeID, Int],
                                       attrOrder: Seq[AttributeID],
                                       cachePos: Seq[(Array[Int], Array[Int])],
                                       cacheSize: Array[Int],
                                       tasksNum: Int)
    extends AbstractPushHCubeJoinExec(
      schema,
      children,
      share,
      CachedLeapFrogAttributeOrderInfo(attrOrder.toArray, cacheSize, cachePos)
    )

case class MergedHCubeLeapJoinExec(schema: RelationSchema,
                                   children: Seq[PhysicalPlan],
                                   share: Map[AttributeID, Int],
                                   attrOrder: Seq[AttributeID],
                                   tasksNum: Int)
    extends AbstractMergedPullHCubeJoinExec(
      schema,
      children,
      share,
      TrieConstructedAttributeOrderInfo(attrOrder.toArray)
    )

abstract class ScanExec(schema: RelationSchema) extends PhysicalPlan {
  override val outputSchema: RelationSchema = schema

  override def getChildren(): Seq[PhysicalPlan] = Seq()

  override def count(): Long = {
    execute().rdd.count()
  }

  override def commOnly(): Long = ???
}

case class DiskScanExec(schema: RelationSchema, dataAddress: String)
    extends ScanExec(schema) {
  override def execute(): Relation = {
    val loader = new DataLoader()
    Relation(schema, loader.csv(dataAddress))
  }

  override def selfString(): String = {
    s"DiskScanExec(schema:${schema}, dataAddress:${dataAddress})"
  }

}

case class InMemoryScanExec(schema: RelationSchema,
                            content: RDD[Array[DataType]])
    extends ScanExec(schema) {
  override def execute(): Relation = {
    Relation(schema, content)
  }

  override def selfString(): String = {
    s"InMemoryScanExec(schema:${schema})"
  }
}
