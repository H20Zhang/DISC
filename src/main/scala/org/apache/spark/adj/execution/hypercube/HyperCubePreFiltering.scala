package org.apache.spark.adj.execution.hypercube

import org.apache.spark.adj.execution.rdd.loader.DataLoader
import org.apache.spark.adj.execution.utlis.SparkSingle


//still testing
class HyperCubePreFiltering {


  val spark = SparkSingle.getSparkSession()


  def testFiltering(): Unit ={


    //    //    Edge1(src1,dst1), Edge2(src1,dst2), Edge3(dst1, dst2), Edge4(dst2, dst3)
//    how the filteredNode will affect triangle query
    val prefix="/user/hzhang/subgraph/Dataset/"
    val input = "enwiki-2013"
    val loader = new DataLoader(prefix+input)
    val rawGraph = loader.EdgeDataset


    val graph = rawGraph.select(rawGraph("_1").as("src")
      , rawGraph("_2").as("dst"))

    graph.createOrReplaceTempView("Graph")


//   select SNode from src1
    val sql0 =
      """
        |select distinct src
        |from Graph
        |where src < 100000
      """.stripMargin

    val src1 = spark.sql(sql0)

    src1.createOrReplaceTempView("SRC1")


// how SNode affect the dst of Edge1 and Edge 2
    val sql1 =
      """
        |select distinct dst
        |from Graph, SRC1
        |where Graph.src = SRC1.src
      """.stripMargin

    val dst1 = spark.sql(sql1)
    val dst2 = spark.sql(sql1)

    dst1.createOrReplaceTempView("DST1")
    dst2.createOrReplaceTempView("DST2")

// how Edge3 are affected

    val sql2 =
      """
        |select distinct Graph.src as dst1, Graph.dst as dst2
        |from Graph, DST1, DST2
        |where Graph.src = DST1.dst and Graph.dst = DST2.dst
      """.stripMargin

    val edge3 = spark.sql(sql2)

//    edge3_1.createOrReplaceTempView("Edge3_1")
//
//    val sql2_2 =
//      """
//        |select distinct dst1, dst2
//        |from Edge3_1, DST2
//        |where dst2 = DST2.dst
//      """.stripMargin
//
//    val edge3 = spark.sql(sql2_2)


//    how Edge4 are affected
    val sql3 =
      """
        |select distinct Graph.src as dst2, Graph.dst as dst3
        |from Graph, DST2
        |where Graph.src = DST2.dst
      """.stripMargin

    val edge4 = spark.sql(sql3)


    println(
      s"graph: ${graph.count()} " +
      s"src1: ${src1.count()} " +
      s"dst1:${dst1.count()} " +
      s"edge3:${edge3.count()} " +
        s"edge4:${edge4.count()} " +
      s"ratio3:${edge3.count().toDouble/graph.count()} " +
      s"ratio4:${edge4.count().toDouble/graph.count()} "
    )









  }



}
