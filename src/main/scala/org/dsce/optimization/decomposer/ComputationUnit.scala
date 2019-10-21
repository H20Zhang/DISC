package org.dsce.optimization.decomposer

import org.dsce.Stage.Stage
import org.dsce.parser.query.Query
import org.dsce.parser.remover.{Element, Equation, EquationSet}
import org.dsce.parser.visualization.VisualizeAble
import org.dsce.{GraphID, IsomorphicAble, Mapping, Stage}
import org.apache.commons.lang.builder.{ToStringBuilder, ToStringStyle}
import org.dsce.parser.graph.HyperNode
import org.dsce.parser.graph.HyperNode
import org.dsce.parser.query.Query
import org.dsce.parser.remover.{Element, Equation, EquationSet}
import org.dsce.parser.visualization.VisualizeAble

import scala.collection.mutable

//TODO: complete below methods
class CUNodeCatelog {

  val PatternToIDMap: mutable.HashMap[CUNode, GraphID] = new mutable.HashMap()
  val IDToPattern: mutable.HashMap[GraphID, CUNode] = new mutable.HashMap()
  var nextID: Int = 0

  def addToCatlog(n: CUNode): GraphID = {
    getID(n) match {
      case Some(id) => id
      case None =>
        PatternToIDMap += ((n, nextID)); IDToPattern += ((nextID, n));
        nextID += 1; nextID - 1
    }
  }

  def getID(n: CUNode): Option[GraphID] = ???

}

object CUNodeCatelog {

  def newInstance() = {
    new CUNodeCatelog
  }
}

class CUSet(val equations: Seq[CUEquation], val stage: Stage) {

  val catelog: CUNodeCatelog = CUNodeCatelog.newInstance()

  def toExecutionPlanSet() = ???

  private def simplifyCUNode() = ???

}

object CUSet {

  def apply(equationSet: EquationSet) = {
    new CUSet(equationSet.equations.map(_.toCUEquation()), Stage.Decomposed)
  }

  def apply(equationSet: EquationSet, stage: Stage) = {
    new CUSet(equationSet.equations.map(_.toCUEquation()), stage)
  }
}

class CUEquation(head: Element, val cuElements: Seq[CUTree], stage: Stage)
    extends Equation(head, cuElements.map(_.element), stage) {}

object CUEquation {
  def apply(eq: Equation, stage: Stage): CUEquation = {
    new CUEquation(eq.head, eq.elements.map(_.toCUTree()), stage)
  }
}

// correspond to the evaluation of one element
class CUTree(val root: CUNode, val element: Element) extends VisualizeAble {
  override def toViz(): String = ???

  override def toString: String = {
    toStringWithIndent(0)
  }

  override def toStringWithIndent(level: Int) = {

    def addIndent(level: Int, str: String) = {
      str
        .split("\n")
        .map(f => s"\n${Seq.fill(level)("  ").foldLeft("")(_ + _)}$f")
        .reduce(_ + _)
        .drop(1)
    }

    val str = s"""
       |CUTree(
       |  element = ${element.toStringWithIndent(1)}
       |  root = ${root.toStringWithIndent(1)})
     """.stripMargin

    addIndent(level, str)
  }
}

object CUTree {

  def apply(root: CUNode, element: Element) = new CUTree(root, element)

  def apply(e: Element): CUTree = {
    ElementDecomposer.decomposeElement(e)
  }
}

class CUNode(val q: Query, val node: HyperNode, val dependency: Seq[CUNode])
    extends IsomorphicAble
    with VisualizeAble {

  override def isIsomorphic(p: IsomorphicAble): Boolean = ???

  override def findIsomorphism(p: IsomorphicAble): Seq[Mapping] = ???

  override def findIsomorphismUnderConstriant(
    p: IsomorphicAble,
    constraint: Mapping
  ): Seq[Mapping] = ???

  override def findAutomorphism(): Seq[Mapping] = ???

  override def findAutomorphismUnderConstriant(
    constraint: Mapping
  ): Seq[Mapping] = ???

  override def toViz(): String = ???

  override def toString: String = {
    toStringWithIndent(0)
  }

  def toStringWithIndent(level: Int): String = {
    val str =
      s"CUNode(\nquery = ${q.toStringWithIndent(1)}\nnode = ${node.toString}\ndependency = ${dependency
        .map(_.toStringWithIndent(1))})"
    addIndent(level, str)
  }

}

object CUNode {
  def apply(q: Query, core: HyperNode, dependency: Seq[CUNode]) =
    new CUNode(q, core, dependency)
}
