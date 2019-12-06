package dsce

import adj.SparkFunSuite
import org.apache.spark.dsce.DISCConf.Mode
import org.apache.spark.dsce.{DISCConf, Query}
import org.apache.spark.dsce.plan.{
  UnOptimizedCountAggregate,
  UnOptimizedSubgraphCount
}
import org.apache.spark.dsce.util.testing.{ExpData, ExpQuery}

class MainTest extends SparkFunSuite {

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

  //check if whole pipeline can be compiled
  test("triangle") {
    val query = "triangle"
    val plan = getPhyiscalPlan(dataset, query)
    println(s"PhyiscalPlan:\n${plan.prettyString()}")

    val output = plan.count()
    println(s"Output Size:${output}")
  }

  //check if whole pipeline can basically work
  test("simple") {
//    val queries = Seq("wedge", "triangle")
    val discConf = DISCConf.defaultConf()
    discConf.mode = Mode.NonInduce

    val queries = Seq("near5Clique")
    queries.foreach { query =>
      val plan = getPhyiscalPlan(dataset, query)
      println(s"plan:\n${plan.prettyString()}")
      val outString = s"""
                         |----------------$query-------------------
                         |plan:\n${plan.prettyString()}
                         |patternSize:${plan.count()}
                         |""".stripMargin
      print(outString)
    }

  }

}
