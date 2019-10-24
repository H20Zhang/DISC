package org.dsce.optimization.subgraph

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.{Catalog, RelationSchema}
import org.apache.spark.adj.plan.{LogicalPlan, UnOptimizedScan}
import org.dsce.optimization.subgraph.Element.State
import org.dsce.plan.{UnOptimizedCountAggregate, UnOptimizedSumAggregate}
import org.dsce.util.Fraction

//TODO: finish this class
class SubgraphCountDecomposer(schemas: Seq[RelationSchema],
                              coreIds: Seq[AttributeID]) {

  val E = schemas
    .map { f =>
      val attrIds = f.attrIDs
      (attrIds(0), attrIds(1))
    }
    .flatMap(f => Seq(f, f.swap))

  val V = E.flatMap(edge => Iterable(edge._1, edge._2)).distinct
  val C = coreIds

  val EToSchemaMap = schemas
    .map { f =>
      val attrIds = f.attrIDs
      ((attrIds(0), attrIds(1)), f)
    }
    .flatMap(f => Seq(f, (f._1.swap, f._2)))
    .toMap

  def initEquation(): Equation = {
    val headElement =
      Element(V, E, C, Fraction(1, 1), State.InducedWithSymmetryBreaked)
    val bodyElement =
      Element(V, E, C, Fraction(1, 1), State.InducedWithSymmetryBreaked)
    val eq = Equation(headElement, Seq(bodyElement))
    eq
  }

  def optimizeEquation(eq: Equation): Equation = {

    //add rule
    val ruleExecutor = new RuleExecutor
    val rule1 = new SymmetryBreakRule
    val rule2 = new InducedToNonInduceRule
    val rule3 = new NonInduceToPartialRule
    val rule4 = new CliqueOptimizeRule
    val rules = Seq(rule1, rule2, rule3, rule4)

    rules.foreach(ruleExecutor.addRule)

    val optimizedRule = ruleExecutor.applyAllRulesTillFix(eq)
    optimizedRule
      .simplify()
  }

  def equationToLogicalPlan(eq: Equation): LogicalPlan = ???

//  {
//    val head = eq.head
//    val body = eq.body
//    val catalog = Catalog.defaultCatalog()
//    val countAggregates = body.map { element =>
//      val schemas = element.E.map(EToSchemaMap)
//      val cores = element.C.map(catalog.getAttribute)
//      UnOptimizedCountAggregate(
//        schemas.map(schema => UnOptimizedScan(schema)),
//        cores
//      )
//    }
//
//    val sumAggregate =
//      UnOptimizedSumAggregate(countAggregates, head.C.map(catalog.getAttribute))
//    sumAggregate
//  }

  //optimize the all logicalPlan
  def genPlan(): LogicalPlan = {
    var eq = initEquation()
    eq = optimizeEquation(eq)
    equationToLogicalPlan(eq)
  }

}
