package disc.integration

import disc.SparkFunSuite
import org.apache.spark.disc.SubgraphCounting
import org.apache.spark.disc.testing.{ExpData, ExpQuery}

class AggregateRuleTest extends SparkFunSuite {

  val dataset = "eu"

  def getPlan(dataset: String, query: String) = {
    val data = ExpData.getDataAddress(dataset)
    val dmlString = new ExpQuery(data) getQuery (query)
    SubgraphCounting.optimizedPhyiscalPlan(dmlString)
  }

  test("wedge") {
    val plan = getPlan(dataset, "wedge")
    println(s"PhysicalMultiplyAgg:\n${plan.prettyString()}")
  }

}
