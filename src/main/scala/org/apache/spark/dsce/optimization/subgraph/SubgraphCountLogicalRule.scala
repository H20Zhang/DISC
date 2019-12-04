package org.apache.spark.dsce.optimization.subgraph

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.plan.{
  LogicalPlan,
  RuleNotMatchedException,
  UnOptimizedScan
}
import org.apache.spark.dsce.optimization.{LogicalRule, subgraph}
import org.apache.spark.dsce.optimization.subgraph.Element.State
import org.apache.spark.dsce.plan
import org.apache.spark.dsce.plan.{
  PartialOrderScan,
  UnOptimizedCountAggregate,
  UnOptimizedSubgraphCount,
  UnOptimizedSumAggregate
}
import org.apache.spark.dsce.util.Fraction

//TODO: debug this
class SubgraphCountLogicalRule() extends LogicalRule {

  def initEquation(schemas: Seq[RelationSchema],
                   coreAttrIds: Seq[AttributeID]): Equation = {

    val E = schemas
      .map { f =>
        val attrIds = f.attrIDs
        (attrIds(0), attrIds(1))
      }
      .flatMap(f => Seq(f, f.swap))

    val V = E.flatMap(edge => Iterable(edge._1, edge._2)).distinct
    val C = coreAttrIds

    val headElement =
      subgraph.Element(
        V,
        E,
        C,
        Fraction(1, 1),
        State.InducedWithSymmetryBreaked
      )
    val bodyElement =
      subgraph.Element(
        V,
        E,
        C,
        Fraction(1, 1),
        State.InducedWithSymmetryBreaked
      )
    val eq = Equation(headElement, Seq(bodyElement))
    eq
  }

  def optimizeEquation(eq: Equation): Equation = {

    //add rule
    val ruleExecutor = new EquationTransformer
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

  def equationToLogicalPlan(
    eq: Equation,
    edgeSchema: Seq[RelationSchema],
    notExistedEdgeSchema: Seq[RelationSchema]
  ): LogicalPlan = {
    val head = eq.head
    val body = eq.body
    val EToSchemaMap = (edgeSchema ++ notExistedEdgeSchema)
      .map { f =>
        val attrIds = f.attrIDs
        ((attrIds(0), attrIds(1)), f)
      }
      .flatMap(f => Seq(f, (f._1.swap, f._2)))
      .toMap

//    println(EToSchemaMap)
//    println(
//      s"edgeSchemas:${edgeSchema}, notIncludedEdgesSchemas:${notExistedEdgeSchema}"
//    )

    val countAggregates = body.map { element =>
      val schemas = element.E.filter { case (u, v) => v > u }.map(EToSchemaMap)
      val coreAttrIds = element.C

      if (element.mode == State.CliqueWithSymmetryBreaked) {
        val edgeRelations = schemas.map { schema =>
          if (schema.attrIDs.intersect(coreAttrIds).isEmpty) {
            PartialOrderScan(schema, (schema.attrIDs(0), schema.attrIDs(1)))
          } else {
            UnOptimizedScan(schema)
          }
        }

        UnOptimizedCountAggregate(edgeRelations, coreAttrIds)
      } else {
        plan.UnOptimizedCountAggregate(
          schemas.map(schema => UnOptimizedScan(schema)),
          coreAttrIds
        )
      }

    }

    val sumAggregate =
      UnOptimizedSumAggregate(countAggregates, eq.body.map(_.factor), head.C)
    sumAggregate
  }

  //optimize the all logicalPlan
  override def apply(plan: LogicalPlan): LogicalPlan = {

    plan match {
      case subgraphPlan: UnOptimizedSubgraphCount => {
        val edgeSchemas = subgraphPlan.edge.map(_.outputSchema)
        val notIncludedEdgesSchemas =
          subgraphPlan.notExistedEdge.map(_.outputSchema)
        val coreAttrIds = subgraphPlan.coreAttrIds
        var eq = initEquation(edgeSchemas, coreAttrIds)
        eq = optimizeEquation(eq)
        equationToLogicalPlan(eq, edgeSchemas, notIncludedEdgesSchemas)
      }
      case _ => throw new RuleNotMatchedException(this)
    }

  }
}
