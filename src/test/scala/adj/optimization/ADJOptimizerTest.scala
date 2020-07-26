package adj.optimization

import adj.SparkFunSuite
import org.apache.spark.adj.optimization.costBased.optimizer.ADJCostOptimizer
import org.apache.spark.adj.utils.exp.ExpQuery
import org.scalatest.FunSuite

class ADJOptimizerTest extends SparkFunSuite {

  val prefix = "./examples/"
  val graphDataAdresses = Map(
    ("eu", "email-Eu-core.txt"),
    ("wikiV", "wikiV.txt"),
    ("debug", "debugData.txt")
  )
  val dataAdress = prefix + graphDataAdresses("wikiV")
  val query = s"house"

  test("adj") {
    val relations = new ExpQuery(dataAdress) getRelations (query)
    val adjOptimizer = new ADJCostOptimizer(relations)
  }
}
