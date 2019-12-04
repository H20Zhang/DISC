package org.apache.spark.dsce.plan

import org.apache.spark.adj.database.Catalog.{AttributeID, DataType}
import org.apache.spark.adj.database.{Relation, RelationSchema}
import org.apache.spark.adj.execution.hcube.pull.{
  HCubePlan,
  PartitionedRelation,
  PullHCube
}
import org.apache.spark.adj.execution.hcube.utils.TriePreConstructor
import org.apache.spark.adj.execution.hcube.{TrieHCubeBlock, TupleHCubeBlock}
import org.apache.spark.adj.execution.subtask.SubTask
import org.apache.spark.adj.optimization.costBased.comp.EnumShareComputer
import org.apache.spark.adj.optimization.stat.Statistic
import org.apache.spark.adj.plan.{InMemoryScanExec, PhysicalPlan, ScanExec}
import org.apache.spark.adj.utils.misc.{Conf, SparkSingle}
import org.apache.spark.dsce.execution.subtask.{
  DSCESubTaskFactory,
  LeapFrogAggregateInfo
}
import org.apache.spark.dsce.util.Fraction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{
  IntegerType,
  LongType,
  StructField,
  StructType
}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.storage.StorageLevel

//TODO: debugging this
case class MultiplyAggregateExec(
  schema: RelationSchema,
  edges: Seq[PhysicalPlan],
  eagerCountTables: Seq[MultiplyAggregateExec],
  lazyCountTables: Seq[Tuple2[Seq[PhysicalPlan], Seq[MultiplyAggregateExec]]],
  subTaskInfo: LeapFrogAggregateInfo,
  coreAttrIds: Seq[AttributeID]
) extends PhysicalPlan {

  override val outputSchema: RelationSchema = schema

  lazy val share = genShare()
  lazy val countTablesRelation = eagerCountTables.map(_.execute())
  lazy val countTableRelationForLazyCountTables =
    lazyCountTables.map(_._2.map(_.execute()))
  lazy val edgeRelations = edges.map(_.execute())
  lazy val edgeRelationsForLazyCountTables =
    lazyCountTables.map(_._1.map(_.execute()))
  lazy val relationsForHCube = countTablesRelation ++ countTableRelationForLazyCountTables
    .flatMap(f => f) ++ edgeRelations ++ edgeRelationsForLazyCountTables
    .flatMap(f => f)
  lazy val countAttrId = schema.attrIDs.diff(coreAttrIds).head

  //share will be computed when this operator is evaluated.
  def genShare(): Map[AttributeID, Int] = {
    //init --- statistics
    val statistic = Statistic.defaultStatistic()

    val inputSchema = relationsForHCube.map(_.schema).zipWithIndex
    val statisticNotCollectedRelationSchema = inputSchema.filter {
      case (schema, index) =>
        statistic.get(schema).isEmpty
    }

    val statisticNotCollectedRelation = statisticNotCollectedRelationSchema
      .map(f => relationsForHCube(f._2))
    statisticNotCollectedRelation.foreach(statistic.add)

    val shareComputer = new EnumShareComputer(
      relationsForHCube.map(_.schema),
      Conf.defaultConf().taskNum
    )
    shareComputer.optimalShare()._1
  }

  //generate the sub-count table using hcube+leapfrogAggregate

  def genHCube(): RDD[SubTask] = {
    //preprocessHCube
    def preprocessHCube(f: PartitionedRelation): PartitionedRelation = {
      val info_ = subTaskInfo
      val rdd = f.partitionedRDD
      val partitioner = f.partitioner

      val preprocessedRDD = rdd.map { block =>
        val tupleHCubeBlock = block.asInstanceOf[TupleHCubeBlock]
        val schema = tupleHCubeBlock.schema
        val content = tupleHCubeBlock.content
        val shareVector = tupleHCubeBlock.shareVector
        var outputBlock = block

        info_ match {
          case s: LeapFrogAggregateInfo => {
            val attrOrders = s.attrIdsOrder
            val triePreConstructor =
              new TriePreConstructor(attrOrders, schema, content)
            val trie = triePreConstructor.construct()
            outputBlock = TrieHCubeBlock(schema, shareVector, trie)
          }
          case _ => throw new Exception(s"not supported taskinfo:${info_}")
        }

        outputBlock
      }

      preprocessedRDD.persist(StorageLevel.MEMORY_ONLY_SER)
      preprocessedRDD.count()

      PartitionedRelation(preprocessedRDD, partitioner)
    }
    //genHCube
    val hcube = new PullHCube(HCubePlan(relationsForHCube, share), subTaskInfo)
    hcube.genPullHCubeRDD(preprocessHCube)
  }

  def genSubCountTable(): DataFrame = {
    val rowRdd = genHCube()
      .flatMap { task =>
        val subJoinTask =
          DSCESubTaskFactory.genSubTask(
            task.shareVector,
            task.blocks,
            task.info
          )

        val iterator = subJoinTask.execute()
        iterator
      }
      .map(f => Row.fromSeq(f))

//    LongType

    val fields = schema.attrIDs.map(
      attrId => StructField(catalog.getAttribute(attrId), IntegerType)
    )

    val dfSchema = StructType(fields)

    println(s"dfSchema:${dfSchema}")

    val spark = SparkSingle.getSparkSession()

    spark.createDataFrame(rowRdd, dfSchema)
  }

  //aggregate the sub-count table
  def aggregateSubCountTable(df: DataFrame): Relation = {
    val groupByAttrs =
      schema.attrIDs
        .diff(Seq(countAttrId))
        .map(catalog.getAttribute)
        .map(attr => df(attr))
    val rdd = df
      .groupBy(groupByAttrs: _*)
      .sum(catalog.getAttribute(countAttrId))
      .rdd
      .map(f => f.toSeq.toArray.map(_.asInstanceOf[DataType]))
    Relation(schema, rdd)
  }

  //aggregate the sub-count table
  def globalAggregate(): Unit = {

    val df = genSubCountTable()

    import org.apache.spark.sql.functions._
    df.agg(sum(catalog.getAttribute(countAttrId))).show()
  }

  override def execute(): Relation = {
    aggregateSubCountTable(genSubCountTable())
  }

  override def count(): Long = ???

  override def commOnly(): Long = ???

  override def getChildren(): Seq[PhysicalPlan] =
    edges ++ eagerCountTables ++ lazyCountTables.flatMap(f => f._1 ++ f._2)

  override def selfString(): String = {
    val coreString =
      coreAttrIds.map(catalog.getAttribute).mkString("(", ", ", ")")
    val eagerCountTableString =
      eagerCountTables.map(_.outputSchema.name).mkString("(", ", ", ")")
    val lazyCountString =
      lazyCountTables
        .map {
          case (edgeRelations, countTables) =>
            (
              edgeRelations.map(_.outputSchema.name).mkString("(", ", ", ")"),
              countTables.map(_.outputSchema.name).mkString("(", ", ", ")")
            )
        }
        .mkString("(", ", ", ")")
    val edgesString = edges.map(_.outputSchema.name).mkString("(", ", ", ")")

    s"MultiplyAggregateExec(core:${coreString}, edges:${edgesString}, eagerTables:${eagerCountTableString}, lazyTable:${lazyCountString}, Info:${subTaskInfo})"
  }
}

//TODO: finish later
case class SumAggregateExec(schema: RelationSchema,
                            countTables: Seq[PhysicalPlan],
                            coefficients: Seq[Fraction],
                            coreAttrIds: Seq[AttributeID])
    extends PhysicalPlan {
  override val outputSchema: RelationSchema = schema

  override def execute(): Relation = ???

  override def count(): Long = ???

  override def commOnly(): Long = ???

  override def getChildren(): Seq[PhysicalPlan] = countTables

  override def selfString(): String = {
    val equationString = coefficients
      .zip(countTables)
      .map {
        case (coeff, countTable) =>
          s"(${coeff}${countTable.outputSchema.name})+"
      }
      .reduce(_ + _)
      .dropRight(1)

    val coreString = coreAttrIds.map(catalog.getAttribute)

    s"SumAggregateExec(core:${coreString}, equation:${equationString}"
  }
}

case class PartialOrderInMemoryScanExec(
  schema: RelationSchema,
  attrWithPartialOrder: (AttributeID, AttributeID),
  content: RDD[Array[DataType]]
) extends ScanExec(schema) {
  override def execute(): Relation = {

    val u = schema.attrIDs.indexOf(attrWithPartialOrder._1)
    val v = schema.attrIDs.indexOf(attrWithPartialOrder._2)

    Relation(schema, content.filter(f => f(u) < f(v)))
  }

  override def selfString(): String = {
    s"PartialOrderInMemoryScanExec(schema:${schema}, partialOrder:${catalog
      .getAttribute(attrWithPartialOrder._1)}<${catalog.getAttribute(attrWithPartialOrder._2)})"
  }
}

case class CachedAggregateExec(originalSchema: RelationSchema,
                               mappedSchema: RelationSchema)
    extends ScanExec(mappedSchema) {

  override def execute(): Relation = {

    Relation(
      mappedSchema,
      InMemoryScanExec(
        originalSchema,
        catalog.getMemoryStore(originalSchema.id.get).get
      ).execute().rdd
    )
  }

  override def selfString(): String = {
    s"CachedInMemoryScan(originalSchema:${originalSchema}, mappedSchema:${mappedSchema})"
  }

}
