package disc.integration

import disc.SparkFunSuite
import org.apache.spark.disc.parser.SubgraphParser
import org.apache.spark.disc.testing.{ExpData, ExpQuery}
import org.apache.spark.sql.Row

class SubgraphParserTest extends SparkFunSuite {
  val data = ExpData.getDataAddress("eu")

  test("parser") {
    val expQuery = new ExpQuery(data)
    val query = expQuery.getQuery("wedge")
    val parser = new SubgraphParser
    val logicalPlan = parser.parseDml(query)
    println(logicalPlan)
    println(s"query:${query}")

    Row.fromSeq(Seq(1, 2))
  }

}
