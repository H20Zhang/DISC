package dsce

import adj.SparkFunSuite
import org.dsce.parser.SubgraphParser
import org.dsce.util.testing.{ExpData, ExpQuery}

class SubgraphParserTest extends SparkFunSuite {
  val data = ExpData.getDataAddress("eu")

  test("parser") {
    val expQuery = new ExpQuery(data)
    val query = expQuery.getQuery("chordalSquare")
    val parser = new SubgraphParser
    val logicalPlan = parser.parseDml(query)
    println(logicalPlan)

  }

}
