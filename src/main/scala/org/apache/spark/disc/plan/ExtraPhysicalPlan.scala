package org.apache.spark.disc.plan

import org.apache.spark.disc.execution.hcube.pull.{
  HCubePlan,
  PartitionedRelation,
  PullHCube
}
import org.apache.spark.disc.execution.hcube.utils.TriePreConstructor
import org.apache.spark.disc.execution.hcube.{TrieHCubeBlock, TupleHCubeBlock}
import org.apache.spark.disc.execution.subtask.{
  LeapFrogAggregateInfo,
  SubTask,
  SubTaskFactory
}
import org.apache.spark.disc.optimization.cost_based.comp.EnumShareComputer
import org.apache.spark.disc.optimization.cost_based.stat.Statistic
import org.apache.spark.disc.catlog.Catalog.{AttributeID, DataType}
import org.apache.spark.disc.catlog.{Relation, Schema}
import org.apache.spark.disc.util.misc.{Conf, Fraction, SparkSingle}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

case class MultiplyAggregateExec(
  schema: Schema,
  edges: Seq[PhysicalPlan],
  eagerCountTables: Seq[PhysicalPlan],
  lazyCountTables: Seq[Tuple2[Seq[PhysicalPlan], Seq[MultiplyAggregateExec]]],
  subTaskInfo: LeapFrogAggregateInfo,
  coreAttrIds: Seq[AttributeID]
) extends PhysicalPlan {

  override val outputSchema: Schema = schema

  lazy val share = genShare()
  lazy val countTablesRelation = eagerCountTables.map(_.execute())
  lazy val countTableRelationForLazyCountTables =
    lazyCountTables.map(_._2.map(_.execute()))

  lazy val edgeRelations = edges.map(_.execute())
  lazy val edgeRelationsForLazyCountTables =
    lazyCountTables.map(_._1.map(_.execute()))
  lazy val relationsForHCube = countTablesRelation ++ edgeRelations ++ edgeRelationsForLazyCountTables
    .flatMap(f => f) ++ countTableRelationForLazyCountTables
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
      Conf.defaultConf().NUM_PARTITION
    )

//    println(s"share:${shareComputer.optimalShare()._1}")

    val share = shareComputer.optimalShare()._1
    share
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
            val attrOrders = s.globalAttrIdsOrder
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
          SubTaskFactory.genSubTask(task.shareVector, task.blocks, task.info)

        val iterator = subJoinTask.execute()
        iterator
      }
      .map(f => Row.fromSeq(f))

    val fields = schema.attrIDs.map(
      attrId => StructField(catalog.getAttribute(attrId), LongType)
    )
    val dfSchema = StructType(fields)
    val spark = SparkSingle.getSparkSession()
    val subCountTable = spark.createDataFrame(rowRdd, dfSchema)

    subCountTable
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
      .map { f =>
        val size = f.length
        val array = new Array[DataType](size)
        var i = 0
        while (i < size) {
          array(i) = f.get(i).asInstanceOf[DataType]
          i += 1
        }
        array
      }

    schema.setContent(rdd)
    Relation(schema, rdd)
  }

  //aggregate the sub-count table
  def globalAggregate(): Unit = {

    val df = genSubCountTable()

    import org.apache.spark.sql.functions._
    df.agg(sum(catalog.getAttribute(countAttrId))).show()
  }

  override def execute(): Relation = {

    val df = genSubCountTable()
    aggregateSubCountTable(df)
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

case class SumAggregateExec(schema: Schema,
                            countTables: Seq[PhysicalPlan],
                            coefficients: Seq[Fraction],
                            coreAttrIds: Seq[AttributeID])
    extends PhysicalPlan {
  override val outputSchema: Schema = schema
  val spark = SparkSingle.getSparkSession()
  lazy val countAttrId = schema.attrIDs.diff(coreAttrIds).head

  def genCoreCountTable(plan: PhysicalPlan) = {
    val relation = plan.execute()
    val schema = relation.schema
    val coreSize = schema.attrIDs.size
    val relationRDD = relation.rdd
    val coreRDD = relationRDD.map { f =>
      val newArray = new Array[DataType](coreSize)
      var i = 0
      while (i < coreSize) {
        newArray(i) = f(i)
        i += 1
      }
      newArray(coreSize - 1) = 0
      (newArray.toSeq.slice(0, coreSize - 1), newArray(coreSize - 1))
    }

    coreRDD.cache()
    coreRDD.count()

    val rdd = relationRDD.map(f => Row.fromSeq(f))
    val relationSchema = relation.schema
    val fields = relationSchema.attrIDs.map(
      attrId => StructField(catalog.getAttribute(attrId), LongType)
    )
    val dfSchema = StructType(fields)

    //    spark.createDataFrame(rdd, dfSchema).join

    spark
      .createDataFrame(rdd, dfSchema)
      .createOrReplaceTempView(relationSchema.name)

    (coreRDD, relationSchema.name)
  }

  def genCountTable(plan: PhysicalPlan,
                    coreRDD: RDD[(Seq[DataType], DataType)]): String = {
    val relation = plan.execute()
    val schema = relation.schema
    val coreSize = schema.attrIDs.size
    val rdd = relation.rdd
//      .map(f => f.toSeq)
      .map(f => (f.toSeq.slice(0, coreSize - 1), f(coreSize - 1)))
      .union(coreRDD)
      .groupByKey()
      .map {
        case (key, counts) =>
          val newArray = new Array[DataType](coreSize)
          var i = 0
          while (i < coreSize - 1) {
            newArray(i) = key(i)
            i += 1
          }
          newArray(coreSize - 1) = counts.sum
          newArray
      }
      .map(f => Row.fromSeq(f))
    val relationSchema = relation.schema
    val fields = relationSchema.attrIDs.map(
      attrId => StructField(catalog.getAttribute(attrId), LongType)
    )
    val dfSchema = StructType(fields)

    spark
      .createDataFrame(rdd, dfSchema)
      .createOrReplaceTempView(relationSchema.name)

    relationSchema.name
  }

  //aggregate the count table
  def aggregateCountTable(tables: Seq[String],
                          coefficients: Seq[Double]): DataFrame = {

    val countAttr = catalog.getAttribute(countAttrId)
    val inputCountAttrs = countTables
      .map(_.outputSchema)
      .map(schema => schema.attrIDs.diff(coreAttrIds).head)
      .map(f => catalog.getAttribute(f))

    val coreString =
      coreAttrIds.map(attrId => catalog.getAttribute(attrId)).mkString(",")

    val splitNum = 5

//    val splitPoint = inputCountAttrs.size / splitNum
    val intermediateTables =
      inputCountAttrs
        .zip(coefficients)
        .zip(tables)
        .grouped(splitNum)
        .zipWithIndex
        .map {
          case (tempInput, idx) =>
            val tempTableName = s"Table${idx}"
            val tempCountName = s"${countAttr}T${idx}"
            val aggregateString =
              tempInput
                .map(_._1)
                .map { f =>
                  if (f._2 == 1.0) {
                    s" (${f._2}*${f._1}) "
                  } else {
                    s" (${f._2}*${f._1}) "
                  }
                }
                .mkString("+")
            val tableString = tempInput.map(_._2).mkString(" natural join ")

            val q =
              s"""
             |select $coreString, $aggregateString as $tempCountName
             |from $tableString
             |""".stripMargin

            println(s"aggregateCountTableSQL: $q")

            val intermediateTable = spark.sql(q)
//            intermediateTable.cache().count()
            intermediateTable.createOrReplaceTempView(tempTableName)

            (tempTableName, tempCountName)
        }

    val tempInput = intermediateTables.toSeq

    val aggregateString =
      tempInput
        .map(_._2)
        .map { f =>
          s" ${f} "
        }
        .mkString("+")
    val tableString = tempInput.map(_._1).mkString(" natural join ")

    println(tempInput)
    val q =
      s"""
         |select $coreString, $aggregateString as $countAttr
         |from $tableString
         |""".stripMargin

    println(s"aggregateCountTableSQL: $q")

    val aggregatedCountTable = spark.sql(q)

//    val aggregateString =
//      inputCountAttrs
//        .zip(coefficients)
//        .map { f =>
//          if (f._2 == 1.0) {
//            s" (${f._2}*${f._1}) "
//          } else {
//            s" (${f._2}*${f._1}) "
//          }
//        }
//        .mkString("+")
//    val tableString = tables.mkString(" natural join ")
//
//    val q =
//      s"""
//         |select $coreString, $aggregateString as $countAttr
//         |from $tableString
//         |""".stripMargin
//
//    println(s"aggregateCountTableSQL: $q")
//
//    val aggregatedCountTable = spark.sql(q)
//
//    println(
//      s"aggregateCountedTable:${aggregatedCountTable.rdd.getNumPartitions}"
//    )

    aggregatedCountTable

  }

  def execute_sparkSql(): Relation = {
    val (coreRDD, coreTable) = genCoreCountTable(countTables(0))
    val tables = countTables.drop(1).map(plan => genCountTable(plan, coreRDD))
    val outputTable =
      aggregateCountTable(coreTable +: tables, coefficients.map(_.toDouble))
    val rdd =
      outputTable.rdd.map(f => f.toSeq.asInstanceOf[Seq[DataType]].toArray)

    schema.setContent(rdd)
    Relation(schema, rdd)
  }

  def count_sparkSql(): Long = {
    val (coreRDD, coreTable) = genCoreCountTable(countTables(0))
    val tables = countTables.drop(1).map(plan => genCountTable(plan, coreRDD))
    val df =
      aggregateCountTable(coreTable +: tables, coefficients.map(_.toDouble))

    import org.apache.spark.sql.functions._
    val count =
      df.agg(sum(catalog.getAttribute(countAttrId))).first().getDecimal(0)

    count.toString.toDouble.toLong
  }

  override def execute(): Relation = {
    execute_sparkSql()
  }

  override def count(): Long = {
    count_sparkSql()
  }

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
  schema: Schema,
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

case class CachedAggregateExec(originalSchema: Schema, mappedSchema: Schema)
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
    s"CachedAggregateExec(originalSchema:${originalSchema}, mappedSchema:${mappedSchema})"
  }

}
