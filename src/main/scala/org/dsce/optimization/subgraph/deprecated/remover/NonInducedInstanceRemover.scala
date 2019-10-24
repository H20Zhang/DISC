package org.dsce.optimization.subgraph.deprecated.remover

import org.dsce.optimization.subgraph.deprecated.graph.Graph
import org.dsce.optimization.subgraph.deprecated.query.Query
import org.dsce.util.GraphBuilder
import org.dsce.util.testing.ExamplePattern
import org.dsce.util.testing.ExamplePattern.PatternName
import org.dsce.{LogAble, Stage, TestAble}

import scala.collection.mutable.ArrayBuffer

object NonInducedInstanceRemover extends TestAble with LogAble {

//  TODO: retest
  def genEquation(eq: Equation): Equation = {
    val elements = eq.elements
    elements
      .map(_genOneEquation(_))
      .reduce((eq, eq2) => eq.replaceElementWithEquation(eq2))
      .toEquationWithStage(Stage.NonInduceInstanceRemoved)
  }

//  return the equations consists of (g:Pattern, 2:Value_to_add_or_delete)
//  TODO: there is problem
  private def _genOneEquation(element: Element): Equation = {

    var eq = Equation(element, Seq(element), Stage.NonInduceInstanceRemoved)
    val inducedElements = _findInducedElements(element)

//    add the negative of the equations of the induced patterns
    inducedElements.foreach { e =>
      val equationsForElement = _genOneEquation(e)
      val negativeEquations = equationsForElement.toNegativeEquation()

      eq = eq.replaceElementWithEquation(negativeEquations)
    }
    eq
  }

  private def _findInducedElements(element: Element): Seq[Element] = {
    val pattern = element.pattern
    val core = element.core
    val symmetryBreakingConditions = element.rules
    val value = element.value

//    when generate undirected edges, remember the source id of is smaller than the destination id
    val patternCliqueEdgeSet = pattern
      .V()
      .combinations(2)
      .map { f =>
        if (f(0) < f(1)) {
          (f(0), f(1))
        } else {
          (f(1), f(0))
        }
      }
      .toSet

    val coreCliqueEdgeSet = core
      .V()
      .combinations(2)
      .map { f =>
        if (f(0) < f(1)) {
          (f(0), f(1))
        } else {
          (f(1), f(0))
        }
      }
      .toSet

    val gEdgeSet = pattern.E().toSet
    val diffEdges =
      patternCliqueEdgeSet.diff(gEdgeSet).diff(coreCliqueEdgeSet).toSeq

    val inducedPatterns = ArrayBuffer[Graph]()

    for (i <- 1 to diffEdges.size) {
//      find the combinations of edges to add
      val edgesCombinations = diffEdges.combinations(i)
      edgesCombinations.foreach { addEdges =>
        val edges = gEdgeSet.toSeq ++ addEdges
        val inducedPattern = GraphBuilder.newGraph(
          edges.flatMap(f => ArrayBuffer(f._1, f._2)).distinct,
          edges
        )
        inducedPatterns += inducedPattern
      }
    }

    inducedPatterns.map(
      g =>
        Element(
          Query(g, core),
          symmetryBreakingConditions,
          Stage.NonInduceInstanceRemoved,
          value
      )
    )
  }

  def test(): Unit = {
    val query = ExamplePattern.queryWithName(PatternName.square)
    val eq = query.toEquation(Stage.SymmetryBreaked)

//    genEquation
    println(s"The equation for remove Non-Induced Instance:${genEquation(eq)}")

    println("1")
  }

}
