package hzhang.test.exp

import org.apache.spark.adj.deprecated.execution.rdd.loader.DataLoader
import org.apache.spark.adj.utils.SparkSingle

object PlayGround {

  val x = 3
  val data = "/user/hzhang/subgraph/Dataset/as_undir"
  val k = 0.057

  lazy val rawEdge = {
    //        new CompactEdgeLoader(data) rawEdgeRDD
    new DataLoader(data) rawEdgeRDD
  }

  lazy val sampledRawEdge = {
    new DataLoader(data) sampledRawEdgeRDD(k)
  }

  val edgeRDD = rawEdge.map(f => (f._1(0), f._1(1))).repartition(200).cache()
  val SampledEdgeRDD = sampledRawEdge.map(f => (f._1(0), f._1(1))).cache()
  val spark = SparkSingle.getSparkSession()

  spark.createDataFrame(SampledEdgeRDD).toDF("A", "B").createOrReplaceTempView("RAB")

  spark.createDataFrame(edgeRDD).toDF("A", "B").createOrReplaceTempView("E")

  spark.sql("cache table RAB")
  spark.sql("cache table E")

  def EdgesForReduction(tables:Seq[(String, String)], edgeTable:String) = {

    val spark = SparkSingle.getSparkSession()

    tables.foreach{f =>
      val sqltxt = s"""
                      |create or replace temporary view R${f._1}${f._2}
                      |as (select ${edgeTable}.A as ${f._1}, ${edgeTable}.B as ${f._2} from ${edgeTable})
        """.stripMargin

      println(sqltxt)
      spark.sql(sqltxt)

    }
  }

  //  example input, attr:A, tables:["RAB", "RAC", "RAD"]
  def ReduceAttr(attr:String, tables:Seq[String], reducerTables:Seq[String]) = {

    val spark = SparkSingle.getSparkSession()

    //    attribute elimination

    val firstTable = reducerTables(0)

    val sqltxt = s"""
                    |create or replace temporary view ${attr}
                    |as (select distinct ${firstTable}.${attr} as ${attr}
                    |    from ${firstTable}
                    |)
       """.stripMargin

    println(sqltxt)
    spark.sql(sqltxt)
    spark.sql(s"cache table ${attr}")

    reducerTables.drop(1).foreach{f =>

      val sqltxt =
        s"""
           |create or replace temporary view ${attr}
           |as (select distinct ${attr}.${attr} as ${attr}
           |    from ${f},${attr}
           |    where ${f}.${attr}=${attr}.${attr}
           |)
       """.stripMargin

      println(sqltxt)
      spark.sql(sqltxt)
    }

    spark.sql(s"cache table ${attr}")

    //    table semi join
    tables.foreach{f =>

      val sqltxt = s"""
                      |create or replace temporary view ${f}
                      |as (select distinct ${f}.*
                      |    from ${f}, ${attr}
                      |    where ${f}.${attr}=${attr}.${attr})
        """.stripMargin

      println(sqltxt)
      spark.sql(sqltxt)
      spark.sql(s"cache table ${f}")

    }
  }


  EdgesForReduction(
    List(
      ("A","C"),
      ("A","D"),
      ("B","C"),
      ("B","D"),
      ("C","D")
    ), "E")

  //    Reduce A

  val t0 = System.nanoTime()
  ReduceAttr("A", Seq("RAB", "RAC", "RAD"), Seq("RAB"))
  spark.sql("select * from RAD").count()

  ReduceAttr("B", Seq("RAB", "RBC", "RBD"), Seq("RAB"))
  ReduceAttr("C", Seq("RAC", "RBC", "RCD"), Seq("RAC", "RBC"))
  ReduceAttr("D", Seq("RAD", "RBD", "RCD"), Seq("RAD", "RBD", "RCD"))

  spark.sql("select * from RAD").count()

  val t1 = System.nanoTime()
  println(s"${(t0 - t1).toDouble/Math.pow(10,9)}")




}
