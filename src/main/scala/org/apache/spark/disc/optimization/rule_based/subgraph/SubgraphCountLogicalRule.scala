package org.apache.spark.disc.optimization.rule_based.subgraph

import org.apache.spark.disc.catlog.Catalog.AttributeID
import org.apache.spark.disc.catlog.Schema
import org.apache.spark.disc.optimization.rule_based.subgraph.Element.State
import org.apache.spark.disc.optimization.rule_based.subgraph
import org.apache.spark.disc.optimization.rule_based.LogicalRule
import org.apache.spark.disc.plan._
import org.apache.spark.disc.util.misc.{Conf, Fraction, QueryType}
import org.apache.spark.disc.plan

class SubgraphCountLogicalRule() extends LogicalRule {

  def initEquation(schemas: Seq[Schema],
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

    val conf = Conf.defaultConf()

    //add rule

    val ruleExecutor = new EquationTransformer
    val rule1 = new SymmetryBreakRule
    val rule2 = new InducedToNonInduceRule
    val rule3 = new NonInduceToPartialRule
    val rule4 = new CliqueOptimizeRule

    conf.queryType match {
      case QueryType.InducedISO => {
        ruleExecutor.addRule(rule1)
        ruleExecutor.addRule(rule2)
//        new ByPassRule(State.Induced, State.NonInduced)
        ruleExecutor.addRule(rule3)
        ruleExecutor.addRule(rule4)

      }

      case QueryType.ISO => {
        ruleExecutor.addRule(rule1)
        ruleExecutor.addRule(new ByPassRule(State.Induced, State.NonInduced))
        ruleExecutor.addRule(rule3)
        ruleExecutor.addRule(rule4)
      }

      case QueryType.HOM => {
        ruleExecutor.addRule(rule1)
        ruleExecutor.addRule(new ByPassRule(State.Induced, State.NonInduced))
        ruleExecutor.addRule(new ByPassRule(State.NonInduced, State.Partial))
        ruleExecutor.addRule(rule4)
      }
      case QueryType.Debug => {
        ruleExecutor.addRule(rule1)
        ruleExecutor.addRule(rule2)
        ruleExecutor.addRule(new ByPassRule(State.NonInduced, State.Partial))
//        ruleExecutor.addRule(rule4)
      }
    }

    val optimizedRule = ruleExecutor.applyAllRulesTillFix(eq)

//    val ruleExecutor2 = new EquationTransformer
//    ruleExecutor2.addRule(rule4)
//    val optimizedRule2 = ruleExecutor2.applyAllRulesTillFix(optimizedRule1)
    optimizedRule
      .simplify()
  }

  def equationToLogicalPlan(eq: Equation,
                            edgeSchema: Seq[Schema],
                            notExistedEdgeSchema: Seq[Schema]): LogicalPlan = {
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

      if (element.state == State.CliqueWithSymmetryBreaked) {
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
