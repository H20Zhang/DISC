package org.apache.spark.adj.utils.testing

import org.apache.spark.adj.optimization.decomposition.graph.GraphBuilder
import org.apache.spark.adj.utils.testing.GraphGenerator.PatternName.PatternName

object GraphGenerator {
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

  object PatternName extends Enumeration {
    type PatternName = Value
    val threeTriangle, square, squareEdge = Value
  }
}
