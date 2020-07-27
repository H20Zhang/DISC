package disc.integration

import disc.SparkFunSuite
import org.apache.spark.disc.testing.{ExpData, ExpQuery}
import org.apache.spark.disc.util.misc.{Conf, Fraction, QueryType}
import org.apache.spark.disc.SubgraphCounting

class SubgraphCountLogicalRuleTest extends SparkFunSuite {

  val dataset = "eu"

  def getEquation(dataset: String, query: String) = {
    val data = ExpData.getDataAddress(dataset)
    val dmlString = new ExpQuery(data) getQuery (query)
    SubgraphCounting.unOptimizedPlan(dmlString).optimize()
  }

  test("debug") {
    Conf.defaultConf().queryType = QueryType.Debug
//    val queries = Seq("DthreePath1", "DthreePath2")
    val queries = Seq("t39")
    queries.foreach { query =>
      val plan = getEquation(dataset, query)
      val outString = s"""
                         |----------------$query-------------------
                         |${plan.prettyString()}
                         |""".stripMargin
      print(outString)
    }
  }

  test("3-node") {
    Conf.defaultConf().queryType = QueryType.Debug
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
    Conf.defaultConf().queryType = QueryType.Debug
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
