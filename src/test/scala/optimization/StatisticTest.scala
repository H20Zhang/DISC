package optimization

import org.apache.spark.adj.optimization.utils.StatisticComputer
import org.apache.spark.adj.utils.testing.RelationGenerator
import org.scalatest.FunSuite

class StatisticTest extends FunSuite{

  test("statistic computer"){
    val relation0 = RelationGenerator.genGraphRelation("R0", "wikiV", Seq("A", "B"))
    val computer = new StatisticComputer(relation0)

    val result = computer.compute()
    println(result)
  }
}
