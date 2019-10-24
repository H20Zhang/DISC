package dsce

import adj.SparkFunSuite
import org.dsce.Query
import org.dsce.optimization.subgraph.{
  CliqueOptimizeRule,
  InducedToNonInduceRule,
  NonInduceToPartialRule,
  RuleExecutor,
  SubgraphCountDecomposer,
  SymmetryBreakRule
}
import org.dsce.plan.UnOptimizedSubgraphCount
import org.dsce.util.Fraction
import org.dsce.util.testing.{ExpData, ExpQuery}
import org.scalatest.FunSuite

class SubgraphCountDecomposerTest extends SparkFunSuite {

  val data = ExpData.getDataAddress("eu")
  val dmlString = "square"
  val dml = new ExpQuery(data) getQuery (dmlString)
  var plan = Query.simpleDml(dml).asInstanceOf[UnOptimizedSubgraphCount]
  val schemas = plan.childrenOps.map(_.outputSchema)
  val coreIds =
    plan.cores.map(coreAttr => plan.catalog.getAttributeID(coreAttr))
  val subgraphOptimizer = new SubgraphCountDecomposer(schemas, coreIds)

  test("initEquation") {
    println(subgraphOptimizer.initEquation())
  }

  test("optimizedEquation") {
    var eq = subgraphOptimizer.initEquation()
    eq = subgraphOptimizer.optimizeEquation(eq)

    println(eq)

  }

  test("fraction") {
    val x = Fraction(1, 2)
    val y = Fraction(2, 3)
    val z = Fraction(-1, 4)
    println(x * z)
  }
}
