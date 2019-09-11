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
  FactorizedAttributeOrderInfo,
  FactorizedLeapFrogJoin,
  FactorizedLeapFrogJoinSubTask,
  LeapFrogJoinSubTask,
  SubTask,
  SubTaskFactory,
  TaskInfo
}
import org.apache.spark.rdd.RDD

//physical plan is the plan that describe the distributed execution process
trait PhysicalPlan {
  def execute(): Relation
  def count(): Long
  def commOnly(): Long
  def getChildren(): Seq[PhysicalPlan]
  val db = Catalog.defaultCatalog()
}

abstract class JoinExec(schema: RelationSchema,
                        @transient children: Seq[PhysicalPlan])
    extends PhysicalPlan {
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

}

//TODO: merge the common part of the following class into HCubeJoinExec, and make
// sure that the Relation Returned by JoinExec have the same schema as the Join.info
abstract class AbstractMergedPullHCubeJoinExec(
  @transient children: Seq[PhysicalPlan],
  share: Map[AttributeID, Int],
  info: TaskInfo
) extends PhysicalPlan
    with Serializable {

  @transient val relations = getChildren().par.map(_.execute()).toArray
  @transient val hcubePlan = HCubePlan(relations, share)
  @transient val hcube = new PullHCube(hcubePlan, info)

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
        case s: AttributeOrderInfo => {
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

  def execute(): Relation = {
    val rdd = hcube
      .genPullHCubeRDD {
        preprocessHCube
      }
      .flatMap { task =>
        val subJoinTask = SubTaskFactory.genMergedSubTask(
          task.shareVector,
          task.blocks.map(_.asInstanceOf[TrieHCubeBlock]),
          task.info
        )

        val iterator = subJoinTask.execute()
        iterator
      }

    val catalog = Catalog.defaultCatalog()
    val schema = RelationSchema(
      s"Temp-R${catalog.nextRelationID()}",
      share.keys.toArray.map(attrId => catalog.getAttribute(attrId))
    )
    catalog.add(schema, rdd)
    Relation(schema, rdd)
  }

  def count() = {
    val num = hcube
      .genPullHCubeRDD(preprocessHCube)
      .map { task =>
        val subJoinTask = SubTaskFactory.genMergedSubTask(
          task.shareVector,
          task.blocks.map(_.asInstanceOf[TrieHCubeBlock]),
          task.info
        )

        val iterator =
          subJoinTask.execute()
        iterator.longSize

      }
      .sum()
      .toLong

    num
  }

  def commOnly(): Long = {
    val num = hcube
      .genPullHCubeRDD(preprocessHCube)
      .map { task =>
        1
      }
      .sum()
    num.toLong
  }

  def getChildren(): Seq[PhysicalPlan] = {
    children
  }
}

abstract class AbstractPullHCubeJoinExec(children: Seq[PhysicalPlan],
                                         share: Map[AttributeID, Int],
                                         info: TaskInfo)
    extends PhysicalPlan {

  val relations = getChildren().map(_.execute()).toArray
  val hcubePlan = HCubePlan(relations, share)
  val hcube = new PullHCube(hcubePlan, info)

  def execute(): Relation = {
    val rdd = hcube
      .genHCubeRDD()
      .flatMap { task =>
        val subJoinTask = SubTaskFactory.genSubTask(
          task.shareVector,
          task.blocks.map(_.asInstanceOf[TupleHCubeBlock]),
          task.info
        )

        val iterator = subJoinTask.execute()
        iterator
      }

    val catalog = Catalog.defaultCatalog()
    val schema = RelationSchema(
      s"Temp-R${catalog.nextRelationID()}",
      share.keys.toArray.map(attrId => catalog.getAttribute(attrId))
    )
    catalog.add(schema, rdd)
    Relation(schema, rdd)
  }

  def count() = {
    val num = hcube
      .genHCubeRDD()
      .map { task =>
        val subJoinTask = SubTaskFactory.genSubTask(
          task.shareVector,
          task.blocks.map(_.asInstanceOf[TupleHCubeBlock]),
          task.info
        )

        val iterator =
          subJoinTask.execute()
        iterator.longSize

      }
      .sum()
      .toLong

    num
  }

  def commOnly(): Long = {
    val num = hcube
      .genHCubeRDD()
      .map { task =>
        1
      }
      .sum()
    num.toLong
  }

  def getChildren(): Seq[PhysicalPlan] = {
    children
  }
}

abstract class AbstractPushHCubeJoinExec(children: Seq[PhysicalPlan],
                                         share: Map[AttributeID, Int],
                                         info: TaskInfo)
    extends PhysicalPlan {

  val relations = getChildren().map(_.execute()).toArray
  val hcubePlan = HCubePlan(relations, share)
  val hcube = new PushHCube(hcubePlan, info)

  def execute(): Relation = {
    val rdd = hcube
      .genHCubeRDD()
      .flatMap { task =>
        val subJoinTask = SubTaskFactory.genSubTask(
          task.shareVector,
          task.blocks.map(_.asInstanceOf[TupleHCubeBlock]),
          task.info
        )

        val iterator = subJoinTask.execute()
        iterator
      }

    val catalog = Catalog.defaultCatalog()
    val schema = RelationSchema(
      s"Temp-R${catalog.nextRelationID()}",
      share.keys.toArray.map(attrId => catalog.getAttribute(attrId))
    )
    catalog.add(schema, rdd)
    Relation(schema, rdd)
  }

  def count() = {
    val num = hcube
      .genHCubeRDD()
      .map { task =>
        val subJoinTask = SubTaskFactory.genSubTask(
          task.shareVector,
          task.blocks.map(_.asInstanceOf[TupleHCubeBlock]),
          task.info
        )

        val iterator =
          subJoinTask.execute()
        iterator.longSize

      }
      .sum()
      .toLong

    num
  }

  def commOnly(): Long = {
    val num = hcube
      .genHCubeRDD()
      .map { task =>
        1
      }
      .sum()
    num.toLong
  }

  def getChildren(): Seq[PhysicalPlan] = {
    children
  }
}

case class PullFactorizedLeapJoinExec(children: Seq[PhysicalPlan],
                                      share: Map[AttributeID, Int],
                                      attrOrder: Seq[AttributeID],
                                      corePos: Int,
                                      tasksNum: Int = 4)
    extends AbstractPullHCubeJoinExec(
      children,
      share,
      FactorizedAttributeOrderInfo(attrOrder.toArray, corePos)
    )

case class PullHCubeLeapJoinExec(children: Seq[PhysicalPlan],
                                 share: Map[AttributeID, Int],
                                 attrOrder: Seq[AttributeID],
                                 tasksNum: Int)
    extends AbstractPullHCubeJoinExec(
      children,
      share,
      AttributeOrderInfo(attrOrder.toArray)
    )

case class PushHCubeLeapJoinExec(children: Seq[PhysicalPlan],
                                 share: Map[AttributeID, Int],
                                 attrOrder: Seq[AttributeID],
                                 tasksNum: Int)
    extends AbstractPushHCubeJoinExec(
      children,
      share,
      AttributeOrderInfo(attrOrder.toArray)
    )

case class MergedHCubeLeapJoinExec(children: Seq[PhysicalPlan],
                                   share: Map[AttributeID, Int],
                                   attrOrder: Seq[AttributeID],
                                   tasksNum: Int)
    extends AbstractMergedPullHCubeJoinExec(
      children,
      share,
      AttributeOrderInfo(attrOrder.toArray)
    )

abstract class ScanExec(schema: RelationSchema) extends PhysicalPlan {
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

}

case class InMemoryScanExec(schema: RelationSchema,
                            content: RDD[Array[DataType]])
    extends ScanExec(schema) {
  override def execute(): Relation = {
    Relation(schema, content)
  }
}
