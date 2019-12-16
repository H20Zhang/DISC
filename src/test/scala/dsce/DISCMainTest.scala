package dsce

import adj.SparkFunSuite
import org.apache.spark.dsce.DISCConf.QueryType
import org.apache.spark.dsce.optimization.aggregate.CountTableCache
import org.apache.spark.dsce.{DISCConf, Query}
import org.apache.spark.dsce.plan.{
  UnOptimizedCountAggregate,
  UnOptimizedSubgraphCount
}
import org.apache.spark.dsce.util.testing.{ExpData, ExpEntry, ExpQuery}

class DISCMainTest extends SparkFunSuite {

  val dataset = "wikiV"

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
    val query = "house"

//    val command1 =
//      s"-q $query -t 43200 -d ${data} -e ShowPlan -u NonInduce -s 100000"
//
//    ExpEntry.main(command1.split("\\s"))

    val command2 =
      s"-d ${data} -q $query -t 43200 -e Count -u NonInduce -c A -s 100000"

    println(command2)
    ExpEntry.main(command2.split("\\s"))
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
