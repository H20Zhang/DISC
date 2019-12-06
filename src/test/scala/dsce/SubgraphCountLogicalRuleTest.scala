package dsce

import adj.SparkFunSuite
import org.apache.spark.dsce.Query
import org.apache.spark.dsce.optimization.subgraph.SubgraphCountLogicalRule
import org.apache.spark.dsce.plan.{
  UnOptimizedCountAggregate,
  UnOptimizedSubgraphCount
}
import org.apache.spark.dsce.util.Fraction
import org.apache.spark.dsce.util.testing.{ExpData, ExpQuery}

class SubgraphCountLogicalRuleTest extends SparkFunSuite {

  val dataset = "eu"

  def getEquation(dataset: String, query: String) = {
    val data = ExpData.getDataAddress(dataset)
    val dmlString = new ExpQuery(data) getQuery (query)
    Query.unOptimizedPlan(dmlString).optimize()
  }

  test("3-node") {
    val queries = Seq("wedge", "triangle")
    queries.foreach { query =>
      val plan = getEquation(dataset, query)
      val outString = s"""
           |----------------$query-------------------
           |${plan.prettyString()}
           |""".stripMargin
      print(outString)
    }

  }

  test("4-node") {
    val queries = Seq(
      "threePath",
      "threeStar",
      "triangleEdge",
      "square",
      "chordalSquare",
      "fourClique"
    )

//    val queries = Seq("threeStar")
    queries.foreach { query =>
      val plan = getEquation(dataset, query)
      val outString = s"""
           |----------------$query-------------------
           |${plan.prettyString()}
           |""".stripMargin

      print(outString)
    }
  }

  test("5-node") {
//    val queries = Seq("house", "threeTriangle", "solarSquare", "near5Clique")
    val queries = Seq("threeTriangle")

    //    val queries = Seq("threeStar")
    queries.foreach { query =>
      val plan = getEquation(dataset, query)
      val outString = s"""
                         |----------------$query-------------------
                         |${plan.prettyString()}
                         |""".stripMargin

      print(outString)
    }
  }

  test("6-node") {
    val queries = Seq(
      "quadTriangle",
      "triangleCore",
      "twinCSquare",
      "twinClique4",
      "starofDavidPlus"
    )

    //    val queries = Seq("threeStar")
    queries.foreach { query =>
      val plan = getEquation(dataset, query)
      val outString = s"""
                         |----------------$query-------------------
                         |${plan.prettyString()}
                         |""".stripMargin

      print(outString)
    }
  }

  test("fraction") {
    val x = Fraction(1, 2)
    val y = Fraction(2, 3)
    val z = Fraction(-1, 4)
    println(x * z)
  }
}
