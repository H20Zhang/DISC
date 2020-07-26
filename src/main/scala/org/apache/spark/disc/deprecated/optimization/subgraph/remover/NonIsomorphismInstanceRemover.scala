package org.apache.spark.disc.deprecated.optimization.subgraph.remover

import org.apache.spark.disc.deprecated.optimization.subgraph.query._
import org.apache.spark.disc.deprecated.optimization.subgraph.remover
import org.apache.spark.disc.testing.ExamplePattern
import org.apache.spark.disc.testing.ExamplePattern.PatternName
import org.apache.spark.disc.util.{ArrayUtil, GraphBuilder}
import org.apache.spark.disc.{LogAble, Stage, TestAble}

import scala.collection.mutable.ArrayBuffer

//  TODO: retest
object NonIsomorphismInstanceRemover extends TestAble with LogAble {

  def genEquation(eq: Equation): Equation = {
    val elements = eq.elements

    elements
      .map(_genOneEquation(_))
      .reduce((eq, eq2) => eq.replaceElementWithEquation(eq2))
      .toEquationWithStage(Stage.NonIsomorphismRemoved)
  }

  def test(): Unit = {
    val squareQuery = ExamplePattern.queryWithName(PatternName.square)
    //    gen equations
    var eq = squareQuery.toEquation(Stage.NonIsomorphismRemoved)
    eq.display()
  }

//  TODO: there is problem
  private def _genOneEquation(element: Element): Equation = {

    var eq = Equation(element, Seq(element), Stage.NonIsomorphismRemoved)
      .mapElement(e => SymmetryBreaker.replaceSymmetryBreakingRule(e))
    val inducedElements = _findNonIsomorphismPatterns(element)

    inducedElements.foreach { e =>
      val equationsForElement = _genOneEquation(e)
      val negativeEquations = equationsForElement.toNegativeEquation()

      eq = eq.replaceElementWithEquation(negativeEquations)
    }
    eq

  }

  private def _findNonIsomorphismPatterns(element: Element): Seq[Element] = {
    val pattern = element.pattern
    val core = element.core
    var symmetryBreakingConditions = element.rules
    val value = element.value

    val pV = pattern.V()
    val pE = pattern.E()
    val cV = core.V()
    val cE = core.E().filter { case (u, v) => u != v }
    val n = pV.size

    val vertexOrderMap = pV.zipWithIndex.toMap
    val orderVertexMap = pV.zipWithIndex.map(_.swap).toMap
    val vertexMappingMap =
      vertexOrderMap.map(f => (f._1, Range(0, f._2 + 1).map(g => Seq(g))))
    var allMappings = Range(0, n)
      .foldLeft(Seq(Seq[Int]()))(
        (prevMappings, vertexId) =>
          ArrayUtil.catersianProduct(
            prevMappings,
            vertexMappingMap(orderVertexMap(vertexId))
        )
      )

//    filter according to edge color
    allMappings = allMappings.filter { mapping =>
      !pE.exists(
        edge =>
          mapping(vertexOrderMap(edge._1)) == mapping(vertexOrderMap(edge._2))
      )
    }

    //    filter one-to-many mapping inside core
    allMappings = allMappings.filter { mapping =>
      cE.forall(
        edge =>
          mapping(vertexOrderMap(edge._1)) != mapping(vertexOrderMap(edge._2))
      )
    }

    //    filter the impossible case due to symmetry breaking condition
    allMappings = allMappings.filter { mapping =>
      element.rules.forall {
        case SymmetryBreakingRule(u, v, isoNodes) =>
          mapping(vertexOrderMap(u)) != mapping(vertexOrderMap(v))
      }
    }

//    filter too large vertex order
    allMappings = allMappings.filter { mapping =>
      val maxOrder = mapping.distinct.size - 1
      !mapping.exists(ele => ele > maxOrder)
    }

    //    println(allMappings)
//    construct pattern
    var allIsoG = allMappings.map { mapping =>
      val newEdgeList = pE.toSeq
        .map(
          edge =>
            (
              orderVertexMap(mapping(vertexOrderMap(edge._1))),
              orderVertexMap(mapping(vertexOrderMap(edge._2)))
          )
        )
        .distinct

      val conds = symmetryBreakingConditions.map(
        f =>
          remover.SymmetryBreakingRule(
            mapping(f.lhs),
            mapping(f.rhs),
            f.isomorphismNodes.map(mapping(_))
        )
      )
      (
        GraphBuilder.newGraph(
          newEdgeList.flatMap(f => ArrayBuffer(f._1, f._2)).distinct,
          newEdgeList
        ),
        conds
      )
    }

    allIsoG = allIsoG.filter(isoG => !pattern.isIsomorphic(isoG._1))

    //    construc new symmetry condition

    allIsoG.map(
      p => Element(Query(p._1, core), p._2, Stage.NonIsomorphismRemoved, value)
    )
  }

}
