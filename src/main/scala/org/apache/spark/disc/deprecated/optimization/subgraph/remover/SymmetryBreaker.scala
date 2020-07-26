package org.apache.spark.disc.deprecated.optimization.subgraph.remover

import org.apache.spark.disc._
import org.apache.spark.disc.deprecated.optimization.aggregate.util.WidthCalculator
import org.apache.spark.disc.deprecated.optimization.subgraph.query.Query
import org.apache.spark.disc.deprecated.optimization.subgraph.remover
import org.apache.spark.disc.util.ArrayUtil
import org.apache.spark.disc.testing.ExamplePattern
import org.apache.spark.disc.testing.ExamplePattern.PatternName

import scala.collection.mutable.ArrayBuffer

object SymmetryBreaker extends TestAble with LogAble {

  def replaceSymmetryBreakingRule(e: Element) = {
    val rules = e.rules
    val IsomorphismNodesToRulesMap =
      rules.map(f => (f.isomorphismNodes, f)).groupBy(_._1).toSeq

    val optimalWidth = WidthCalculator.fhtw(e.pattern)

    val widthUnderRules = IsomorphismNodesToRulesMap.map { f =>
      val subRules = f._2.map(_._2)
      val width = WidthCalculator.fhtwUnderConstraint(e.pattern, subRules)
      (subRules, width == optimalWidth, width)
    }

    logger.debug(s"widthUnderRules: ${widthUnderRules}")
    logger.debug(s"optimalWidth:${optimalWidth}")

    var scaleFactor = 1.0

    val acceptableRules = widthUnderRules
      .filter {
        case (subRules, isRetain, _) =>
          if (!isRetain) {
            scaleFactor = scaleFactor * Range(
              1,
              subRules.head.isomorphismNodes.size + 1
            ).reduce(_ * _)
          }

          isRetain
      }
      .flatMap(f => f._1)

    Element(e.q, acceptableRules, e.stage, e.value / scaleFactor)
  }

  def genEquation(eq: Equation) = {
    eq.mapElement(e => genElementWithOptimalConditions(e.q))
      .mapElement(e => replaceSymmetryBreakingRule(e))
      .toEquationWithStage(Stage.SymmetryBreaked)
  }

  private def genElementWithOptimalConditions(q: Query) = {
    val conds = genElementOfAllConditions(q)

    conds.isEmpty match {
      case true => {
        Element(q, ArrayBuffer(), Stage.SymmetryBreaked)
      }
      case false => {
        conds
          .map(
            e => (e, WidthCalculator.fhtwUnderConstraint(e.pattern, e.rules))
          )
          .minBy(_._2)
          ._1
      }
    }
  }

//  find all symmetry breaking condition
  private def genElementOfAllConditions(q: Query) = {

    //    find all automorphisms
    val validMappings = q.findAutomorphism()

    //    find symmetry breaking conditions for each node.
    val remainNodes = q.pattern.V().diff(q.core.V())
    val matchingOrders = ArrayUtil.enumerations(remainNodes)

    //    find possible symmetry breaking conditions
    val possibleConditions = matchingOrders.map { order =>
      val conditions = ArrayBuffer[SymmetryBreakingRule]()
      for (i <- order) {
        val matches = matchesForNode(i, conditions, validMappings)
        conditions ++= genCondition(matches)
      }
      conditions
    }

    possibleConditions.distinct.map(f => Element(q, f, Stage.SymmetryBreaked))
  }

//  find equivalent nodes for a specific node (not counting itself)
  private def matchesForNode(node: NodeID,
                             conditions: Seq[SymmetryBreakingRule],
                             mappings: Seq[Mapping]): Seq[NodeID] = {
//    filter the mapping that doesn't satisfy the condition

    val validMappings = mappings.filter { mapping =>
      if (conditions.isEmpty) {
        true
      } else {
        conditions.forall(cond => mapping(cond.lhs) < mapping(cond.rhs))
      }
    }

    val eqNodes = validMappings
      .map(f => f.toSeq.map(_.swap).toMap.get(node).get)
      .distinct

    eqNodes
  }

//  each condition (a, b) represents a < b
  private def genCondition(matches: Seq[NodeID]): Seq[SymmetryBreakingRule] = {

    val sortedIsoNodes = matches.sorted

    if (matches.size == 1) {
      Seq[SymmetryBreakingRule]()
    } else {
      val head = matches.sorted.head
      matches.sorted.drop(1).map { f =>
        remover.SymmetryBreakingRule(head, f, sortedIsoNodes)
      }
    }
  }

  def test(): Unit = {

    val q = ExamplePattern.queryWithName(PatternName.threeTriangle)

//    val eq = findConditions(g1)
    val eq = genEquation(q.toEquation(Stage.Unprocessed))

    println(eq)

  }
}
