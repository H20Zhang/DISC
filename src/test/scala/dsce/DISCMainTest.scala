package dsce

import adj.SparkFunSuite
import org.apache.spark.dsce.DISCConf.QueryType
import org.apache.spark.dsce.optimization.aggregate.CountTableCache
import org.apache.spark.dsce.{DISCConf, Query}
import org.apache.spark.dsce.plan.{
  UnOptimizedCountAggregate,
  UnOptimizedSubgraphCount
}
import org.apache.spark.dsce.testing.{ExpData, ExpEntry, ExpQuery}

class DISCMainTest extends SparkFunSuite {

//  ("facebook", "facebook.txt"),
//  ("reactcome", "reactcome.txt"),
//  ("as-caida", "as-caida.txt"),
//  ("to", "topology.txt"),

//  val dataset = "reactcome"
  val dataset = "eu"

  def getPhyiscalPlan(dataset: String, query: String) = {
    val data = ExpData.getDataAddress(dataset)
    val dmlString = new ExpQuery(data) getQuery (query)
    Query.optimizedPhyiscalPlan(dmlString)
  }

  def getLogicalPlan(dataset: String, query: String) = {
    val data = ExpData.getDataAddress(dataset)
    val dmlString = new ExpQuery(data) getQuery (query)
    Query.optimizedLogicalPlan(dmlString)
  }

  test("expEntry") {
    val data = ExpData.getDataAddress(dataset)
//    val executeMode = "Count"
    val queryType = "Induce"
    val executeMode = "ShowPlan"

//    val queries =
//      Seq("quadTriangle", "triangleCore", "twinCSquare", "twinClique4")

//    val queries =
//      Seq("g13")

//    val queries = Seq("t7", "t8", "t9")

//    val queries = Range(13, 19) ++ Range(20, 46) map (id => s"g${id}")

//    println(queries)
//    val queries =
//      Seq("house", "threeTriangle", "near5Clique", "solarSquare")
    //    val queries =
    //      Seq("threePath", "threeStar", "square", "triangleEdge", "chordalSquare")
    val queries =
      Seq("4profile")
//    val queries =
//      Seq("Dwedge1", "Dwedge2")
//    val queries =
//      Seq("DchordalSquare1", "DchordalSquare2")
//    val queries =
//      Seq("t3", "t4", "t5", "t6")
//    val queries =
//      Seq("square", "t38", "t39")
//    val queries =
//      Seq("DthreePath1", "DthreePath2")
//    val queries =
//      Seq("DtriangleEdge1", "DtriangleEdge2", "DtriangleEdge3")
//    val queries =
//      Seq("DtriangleEdge2")

    queries.foreach { query =>
//      CountTableCache.reset()
      val command1 =
        s"-q $query -t 43200 -d ${data} -e $executeMode -u ${queryType} -s 100000 -c A -p Single"

      ExpEntry.main(command1.split("\\s"))
    }
  }

  //check if whole pipeline can be compiled
  test("triangle") {
    val query = "triangle"
    val plan = getPhyiscalPlan(dataset, query)
    println(s"PhyiscalPlan:\n${plan.prettyString()}")

    val output = plan.count()
    println(s"Output Size:${output}")
  }

  test("3-node") {
    //check if whole pipeline can basically work

    val queries = Seq("wedge", "triangle")
    val discConf = DISCConf.defaultConf()
    discConf.queryType = QueryType.Induce

    queries.foreach { query =>
      val plan = getPhyiscalPlan(dataset, query)
      //      println(s"plan:\n${plan.prettyString()}")
      val outString = s"""
                           |----------------$query-------------------
                           |plan:\n${plan.prettyString()}
                           |patternSize:${plan.count()}
                           |""".stripMargin
      print(outString)
    }
  }

  test("4-node") {
    //check if whole pipeline can basically work

    val queries = Seq(
      "threePath",
      "threeStar",
      "triangleEdge",
      "square",
      "chordalSquare",
      "fourClique"
    )
    val discConf = DISCConf.defaultConf()
    discConf.queryType = QueryType.Induce

    queries.foreach { query =>
      val plan = getPhyiscalPlan(dataset, query)
      //      println(s"plan:\n${plan.prettyString()}")
      val outString = s"""
                         |----------------$query-------------------
                         |plan:\n${plan.prettyString()}
                         |""".stripMargin
      print(outString)
    }
  }

  //check if whole pipeline can basically work
  test("5-node") {
    //    val queries = Seq("wedge", "triangle")
    val discConf = DISCConf.defaultConf()
    discConf.queryType = QueryType.NonInduce

    val queries = Seq("house", "threeTriangle", "solarSquare", "near5Clique")
//    val queries = Seq("solarSquare")
//    val queries = Seq("house")
    queries.foreach { query =>
      CountTableCache.reset()
      val plan = getPhyiscalPlan(dataset, query)
//      println(s"plan:\n${plan.prettyString()}")
      val outString = s"""
                         |----------------$query-------------------
                         |plan:\n${plan.prettyString()}
                         |patternSize:${plan.count()}
                         |""".stripMargin
      print(outString)
    }
  }

  //check if whole pipeline can basically work
  test("6-node") {
//    val queries = Seq("wedge", "triangle")
    val discConf = DISCConf.defaultConf()
    discConf.queryType = QueryType.NonInduce

    val queries = Seq(
      "quadTriangle",
      "triangleCore",
      "twinCSquare",
      "twinClique4",
      "starofDavidPlus"
    )
    queries.foreach { query =>
      CountTableCache.reset()
      val plan = getPhyiscalPlan(dataset, query)
      println(s"plan:\n${plan.prettyString()}")
//      val outString = s"""
//                         |----------------$query-------------------
//                         |plan:\n${plan.prettyString()}
//                         |patternSize:${plan.count()}
//                         |""".stripMargin
//      print(outString)
    }
  }

}
