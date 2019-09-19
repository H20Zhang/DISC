package org.apache.spark.adj.utils.exp

import org.apache.spark.adj.database.{Catalog, RelationSchema}
import org.apache.spark.adj.utils.misc.SparkSingle

class SparkSQLExecutor(schemas: Seq[RelationSchema]) {

  val spark = SparkSingle.getSparkSession()
  val sc = SparkSingle.getSparkContext()

  def SparkSQLResult(): Long = {
    import spark.implicits._

    schemas.foreach {
      case schema =>
        val rdd = Catalog.defaultCatalog().getMemoryStore(schema.id.get).get
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

    println(sqlQuery)
    //    spark.catalog.listTables().show()
    spark.sql(sqlQuery).count()
  }
}
