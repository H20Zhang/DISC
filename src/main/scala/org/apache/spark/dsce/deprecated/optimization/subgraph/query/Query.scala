package org.apache.spark.dsce.deprecated.optimization.subgraph.query

import org.apache.spark.dsce.Stage.Stage
import org.apache.spark.dsce._
import org.apache.spark.dsce.deprecated.optimization.subgraph.graph.Graph
import org.apache.spark.dsce.deprecated.optimization.subgraph.remover
import org.apache.spark.dsce.deprecated.optimization.subgraph.remover._
import org.apache.spark.dsce.deprecated.optimization.subgraph.visualization.VisualizeAble

class Query(val pattern: Graph, val core: Graph)
    extends VisualizeAble
    with IsomorphicAble
    with LogAble {

  def containEdge(e: Edge) = {
    pattern.containEdge(e)
  }

  def containNode(v: NodeID) = {
    pattern.containNode(v)
  }

  override def isIsomorphic(q: IsomorphicAble) = {
    !findIsomorphism(q).isEmpty
  }

  override def findIsomorphism(q: IsomorphicAble) = {
    q match {
      case query: Query => {
        val coreMappings = core.findIsomorphism(query.core)
        coreMappings.flatMap(
          coreMapping =>
            pattern.findIsomorphismUnderConstriant(query.pattern, coreMapping)
        )
      }
      case _ =>
        throw new Exception(
          "cannot compare isomoprhic objects with different class"
        )
    }
  }

  override def findAutomorphism() = {
    pattern.findAutomorphismUnderConstriant(core.V().map(id => (id, id)).toMap)
  }

  override def findIsomorphismUnderConstriant(
    p: IsomorphicAble,
    constraint: Mapping
  ): Seq[Mapping] = {
    throw new UnsupportedOperationException
  }

  override def findAutomorphismUnderConstriant(
    constraint: Mapping
  ): Seq[Mapping] = {
    throw new UnsupportedOperationException
  }

  def toEquation(stage: Stage): Equation = {
    stage match {
      case Stage.Unprocessed => {
        Equation(toElement(), toElement(), Stage.Unprocessed)
      }
      case Stage.SymmetryBreaked => {
        SymmetryBreaker.genEquation(toEquation(Stage.Unprocessed))
      }
      case Stage.NonInduceInstanceRemoved => {
        NonInducedInstanceRemover.genEquation(
          toEquation(Stage.NonInduceInstanceRemoved)
        )
      }
      case Stage.NonIsomorphismRemoved => {
        NonIsomorphismInstanceRemover.genEquation(
          toEquation(Stage.NonIsomorphismRemoved)
        )
      }
    }
  }

  def toElement(): Element = {
    remover.Element(this)
  }

  override def toString: String = {
    toStringWithIndent(0)
  }

  override def toStringWithIndent(level: Int) = {

    def addIndent(level: Int, str: String) = {
      str
        .split("\n")
        .map(f => s"\n${Seq.fill(level)("\t").foldLeft("")(_ + _)}$f")
        .reduce(_ + _)
        .drop(1 + level)
    }

    val str = s"Query(\npattern = ${pattern.toString}\ncore = ${core.toString})"

    addIndent(level, str)
  }

  override def toViz(): String = {

    val allNodesToIDMap =
      pattern.V().map(id => (id, s"node${objID}${id}")).toMap

    val coreNodesToIDMap = core.V().map(id => (id, s"node${objID}${id}")).toMap
    val coreEdgesOfNodeIDList =
      core.E().map { case (u, v) => (allNodesToIDMap(u), allNodesToIDMap(v)) }

    val pNodesToIDMap =
      pattern.V().diff(core.V()).map(id => (id, s"node${objID}${id}")).toMap
    val pEdgesOfNodeIDList = pattern.E().diff(core.E()).map {
      case (u, v) => (allNodesToIDMap(u), allNodesToIDMap(v))
    }

    s"""
       |subgraph cluster_$objID {
       |		label = "Node ID:v$objID";
       |		edge [dir=none];
       |
       |    ${coreNodesToIDMap
         .map(id => s"""${id._2}[label="${id._1}",color=red,style=filled];\n""")
         .reduce(_ + _)}
       |    ${pNodesToIDMap
         .map(id => s"""${id._2}[label="${id._1}"];\n""")
         .reduce(_ + _)}
       |		${coreEdgesOfNodeIDList
         .map { case (u, v) => s"$u -> $v[color=red,style=filled];\n" }
         .reduce(_ + _)}
       |    ${pEdgesOfNodeIDList
         .map { case (u, v) => s"$u -> $v;\n" }
         .reduce(_ + _)}
       |}
       |
       |
     """.stripMargin
  }
}

object Query {
  def apply(pattern: Graph, core: Graph): Query = new Query(pattern, core)
}
