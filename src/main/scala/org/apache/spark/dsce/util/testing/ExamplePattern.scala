package org.apache.spark.dsce.util.testing

import org.apache.spark.dsce.Stage
import org.apache.spark.dsce.deprecated.optimization.subgraph.remover.{
  Element,
  SymmetryBreakingRule
}
import org.apache.spark.dsce.util.testing.ExamplePattern.PatternName.PatternName
import org.apache.spark.dsce.util.{GraphBuilder, QueryBuilder}

import scala.collection.mutable.ArrayBuffer

// Some example patterns
object ExamplePattern {

  lazy val square = {
    val p = "0-1;1-2;2-3;0-3"
    val c = "0-0"
    (p, c)
  }
  lazy val squareEdge = {
    val p = "0-1;1-2;2-3;0-3;2-4"
    val c = "0-0"
    (p, c)
  }

  def graphWithName(name: PatternName) = name match {
    case PatternName.threeTriangle => GraphBuilder.newGraph(threeTriangle._1)
    case PatternName.square        => GraphBuilder.newGraph(square._1)
    case PatternName.squareEdge    => GraphBuilder.newGraph(squareEdge._1)
    case _                         => throw new Exception(s"No such pattern with name:$name")
  }

  lazy val threeTriangle = {
    val p = "0-1;0-2;1-2;0-3;1-3;0-4;2-4"
    val c = "0-0"
    (p, c)
  }

  def elementWithName(name: PatternName) = name match {
    case PatternName.threeTriangle =>
      Element(
        queryWithName(PatternName.threeTriangle),
        Seq(SymmetryBreakingRule(1, 2, ArrayBuffer(1, 2))),
        Stage.SymmetryBreaked,
        1
      )
    case PatternName.square =>
      Element(
        queryWithName(PatternName.square),
        Seq(SymmetryBreakingRule(1, 3, ArrayBuffer(1, 3))),
        Stage.SymmetryBreaked,
        1
      )
    case PatternName.squareEdge =>
      Element(
        queryWithName(PatternName.squareEdge),
        Seq(SymmetryBreakingRule(1, 3, ArrayBuffer(1, 3))),
        Stage.SymmetryBreaked,
        1
      )
    case _ => throw new Exception(s"No such pattern with name:$name")
  }

  def queryWithName(name: PatternName) = name match {
    case PatternName.threeTriangle =>
      QueryBuilder.newQuery(threeTriangle._1, threeTriangle._2)
    case PatternName.square => QueryBuilder.newQuery(square._1, square._2)
    case PatternName.squareEdge =>
      QueryBuilder.newQuery(squareEdge._1, squareEdge._2)
    case _ => throw new Exception(s"No such pattern with name:$name")
  }

  object PatternName extends Enumeration {
    type PatternName = Value
    val threeTriangle, square, squareEdge = Value
  }
}
