package org.apache.spark.adj.utils.testing

import org.apache.spark.adj.database.Catalog.DataType
import org.apache.spark.adj.database.{Catalog, Query, Relation, RelationSchema}
import org.apache.spark.adj.execution.hcube.TupleHCubeBlock
import org.apache.spark.adj.execution.leapfrog.{Alg, ArraySegment}
import org.apache.spark.adj.plan.{AttributeOrderInfo, LeapFrogJoinSubTask}
import org.apache.spark.adj.utils.misc.SparkSingle
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.util.Random

abstract class JoinValidator(contents: Seq[Array[Array[DataType]]],
                             schemas: Seq[RelationSchema]) {

  val spark = SparkSingle.getSparkSession()
  val sc = SparkSingle.getSparkContext()

  def SparkSQLResult(): Long = {
    import spark.implicits._

    schemas.zip(contents).foreach {
      case (schema, content) =>
        val rdd = sc.parallelize(content)
        val attrName = schema.attrs
        val df = schema.attrs.size match {
          case 1 => rdd.map(f => f(0)).toDF(attrName: _*)
          case 2 => rdd.map(f => (f(0), f(1))).toDF(attrName: _*)
          case 3 => rdd.map(f => (f(0), f(1), f(2))).toDF(attrName: _*)
          case 4 => rdd.map(f => (f(0), f(1), f(2), f(3))).toDF(attrName: _*)
          case 5 =>
            rdd.map(f => (f(0), f(1), f(2), f(3), f(4))).toDF(attrName: _*)
        }

        df.createOrReplaceTempView(schema.name)
    }

    val naturalJoin = " natural join "

    val sqlQuery =
      s"""
         |select *
         |from ${schemas
           .map(_.name + naturalJoin)
           .reduce(_ + _)
           .dropRight(naturalJoin.size)}
         |""".stripMargin

//    spark.catalog.listTables().show()
    spark.sql(sqlQuery).count()
  }

  def methodToTest(): Long

  def validate() = {

    println(s"--------------------------")
    println(s"query information")

    val catalog = Catalog.defaultCatalog()
    val contentRDDs = contents.map(f => sc.parallelize(f))

    schemas.zip(contentRDDs).foreach {
      case (schema, content) =>
        catalog.addOrReplaceContent(schema, content)
    }

    schemas.foreach { schema =>
      println(schema)
    }

    val result1 = SparkSQLResult()
    val result2 = methodToTest()

    val isPass = result1 == result2
    if (!isPass) {
//      contents.foreach { content =>
//        println(content.toSeq.map(_.toSeq))
//      }
//      schemas.foreach { schema =>
//        val name = schema.name
//        spark.sql(s"select * from ${name}").show(1000)
//      }
    }

//    Catalog.reset()
    println(s"true result:${result1}, methodToTest result ${result2}")

    isPass
  }
}

class HCubeJoinValidator(contents: Seq[Array[Array[DataType]]],
                         schemas: Seq[RelationSchema])
    extends JoinValidator(contents, schemas) {
  override def methodToTest(): Long = {

    val queryString =
      s"Join ${schemas.map(schema => s"${schema.name};").reduce(_ + _).dropRight(1)} "

    Query.countQuery(queryString)
  }
}

class LeapFrogJoinValidator(contents: Seq[Array[Array[DataType]]],
                            schemas: Seq[RelationSchema])
    extends JoinValidator(contents, schemas) {

  override def methodToTest(): Long = {
    val attrIds = schemas.flatMap(_.attrIDs).distinct.toArray
    val shareVector = attrIds.zipWithIndex.toMap

    val HCubeTupleBlocks = schemas.zip(contents).map {
      case (schema, content) =>
        TupleHCubeBlock(
          schema,
          schema.attrIDs.map(shareVector).toArray,
          content
        )
    }

    val attrOrder = Random.shuffle(attrIds.toSeq).toArray
    val info = AttributeOrderInfo(attrOrder)
    val subjoin =
      new LeapFrogJoinSubTask(
        shareVector.values.toArray,
        HCubeTupleBlocks,
        info
      )

    val leapIt = subjoin.execute()
    var count = 0
    while (leapIt.hasNext) {
      count += 1
      val tuple = leapIt.next()
    }
    count
  }
}

class LeapFrogUnaryValidator(contents: Seq[Array[DataType]],
                             schemas: Seq[RelationSchema])
    extends JoinValidator(contents.map(f => f.map(g => Array(g))), schemas) {

  val lists = contents.map(f => ArraySegment(f)).toArray

  override def methodToTest(): Long = {
    val it = Alg.leapfrogIt(lists)
    it.hasNext
    val result = it.toArray.toSeq
//    println(result)
    result.size
  }

  def intersectionIterator(): Long = {
    val it = Alg.listIt(lists)
    it.hasNext
    val result = it.toArray.toSeq
//    println(result)
    result.size
  }

  override def validate(): Boolean = {
    val result2 = methodToTest()
    val result1 = intersectionIterator()

    val isPass = result1 == result2
    println(s"result1:${result1}, result2:${result2}")
    assert(isPass)

    isPass
  }
}
