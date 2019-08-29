package org.apache.spark.adj.utils.testing

import org.apache.spark.adj.database.Catalog.DataType
import org.apache.spark.adj.database.{Query, Relation, RelationSchema}
import org.apache.spark.adj.hcube.TupleHCubeBlock
import org.apache.spark.adj.plan.{AttributeOrderInfo, SubJoin}
import org.apache.spark.adj.utils.SparkSingle
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.util.Random



abstract class JoinValidator(contents:Seq[Array[Array[DataType]]], schemas:Seq[RelationSchema]) {

  val spark = SparkSingle.getSparkSession()
  val sc = SparkSingle.getSparkContext()

  def SparkSQLResult():Long = {
    import spark.implicits._

    schemas.zip(contents).foreach{
      case (schema, content) =>

        val rdd = sc.parallelize(content)
        val attrName = schema.attrs
        val df = schema.attrs.size match {
          case 1 => rdd.map(f => f(0)).toDF(attrName:_*)
          case 2 => rdd.map(f => (f(0), f(1))).toDF(attrName:_*)
          case 3 => rdd.map(f => (f(0), f(1), f(2))).toDF(attrName:_*)
          case 4 => rdd.map(f => (f(0), f(1), f(2), f(3))).toDF(attrName:_*)
          case 5 => rdd.map(f => (f(0), f(1), f(2), f(3), f(4))).toDF(attrName:_*)
        }

        df.createOrReplaceTempView(schema.name)
    }

    val naturalJoin = " natural join "

    val sqlQuery =
      s"""
         |select *
         |from ${schemas.map(_.name + naturalJoin).reduce(_+_).dropRight(naturalJoin.size)}
         |""".stripMargin

    spark.catalog.listTables().show()
    spark.sql(sqlQuery).count()
  }

  def methodToTest():Long

  def validate() = {
    methodToTest() == SparkSQLResult()
  }
}

class HCubeJoinValidator(contents:Seq[Array[Array[DataType]]], schemas:Seq[RelationSchema]) extends JoinValidator(contents, schemas) {
  override def methodToTest(): Long = ???
}



class LeapFrogJoinValidator(contents:Seq[Array[Array[DataType]]], schemas:Seq[RelationSchema]) extends JoinValidator(contents, schemas) {

  override def methodToTest(): Long = {
    val attrIds = schemas.flatMap(_.attrIDs).distinct.toArray
    val shareVector = attrIds.zipWithIndex.toMap

    val HCubeTupleBlocks = schemas.zip(contents).map{
      case (schema, content) =>
        TupleHCubeBlock(schema, schema.attrIDs.map(shareVector).toArray, content)
    }

    val attrOrder = Random.shuffle(attrIds.toSeq).toArray
    val info = AttributeOrderInfo(attrOrder)
    val subjoin = new SubJoin(shareVector.values.toArray, HCubeTupleBlocks, info)

    val leapIt = subjoin.execute()
    var count = 0
    while (leapIt.hasNext){
      count += 1
      val tuple = leapIt.next()
    }
    count
  }
}