package optimization

import org.apache.spark.adj.optimization.optimizer.ADJOptimizer
import org.apache.spark.adj.utils.exp.ExpQuery
import org.scalatest.FunSuite
import utils.SparkFunSuite

class ADJOptimizerTest extends SparkFunSuite {

  val prefix = "./examples/"
  val graphDataAdresses = Map(
    ("eu", "email-Eu-core.txt"),
    ("wikiV", "wikiV.txt"),
    ("debug", "debugData.txt")
  )
  val dataAdress = prefix + graphDataAdresses("debug")
  val query = s"house"

  test("adj") {
    val relations = new ExpQuery(dataAdress) getRelations (query)
    val adjOptimizer = new ADJOptimizer(relations)
    adjOptimizer.debug()

  }
}
