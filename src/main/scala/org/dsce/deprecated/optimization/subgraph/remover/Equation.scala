package org.dsce.deprecated.optimization.subgraph.remover

import org.dsce.Stage.Stage
import org.dsce.deprecated.optimization.aggregate.{CUEquation, CUSet, CUTree}
import org.dsce.deprecated.optimization.subgraph.query.{Query, QuerySet}
import org.dsce.deprecated.optimization.subgraph.visualization.VisualizeAble
import org.dsce.{IsomorphicAble, NodeID, Stage}
//import org.apache.commons.lang.builder.{ToStringBuilder, ToStringStyle}

import scala.collection.mutable.ArrayBuffer

class EquationSet(val equations: Seq[Equation], val stage: Stage) {
  def toComputationUnits(): CUSet = {
    CUSet(this, Stage.Decomposed)
  }
}

object EquationSet {
  def apply(querySet: QuerySet): EquationSet = {
    apply(querySet, Stage.Unprocessed)
  }

  def apply(querySet: QuerySet, stage: Stage) = {
    val eqs = querySet.queries.map(_.toEquation(stage))
    new EquationSet(eqs, stage)
  }
}

class Equation(val head: Element, val elements: Seq[Element], val stage: Stage)
    extends VisualizeAble {

  def mapElement(func: Element => Element) = {
    Equation(head, elements.map(f => func(f)), stage)
  }

  def flatMapElement(func: Element => Iterable[Element]) = {
    Equation(head, elements.flatMap(f => func(f)), stage)
  }

  def filterElement(func: Element => Boolean) = {
    Equation(head, elements.filter(f => func(f)), stage)
  }

  def addElement(element: Element): Equation = {
    Equation(head, elements :+ element, stage).simplifyEquation()
  }

  def replaceElementWithEquation(eq: Equation): Equation = {
    eq.stage == stage match {
      case true => {
        val replacedElements1 = elements.filter(e => !e.isIsomorphic(eq.head))
        Equation(head, replacedElements1 ++ eq.elements, stage)
          .simplifyEquation()
      }
      case false => {
        throw new Exception("Stage of equation to merge doesn't match")
      }
    }
  }

  def simplifyEquation(): Equation = {
    var remainEquations = elements
    val simplifiedElements = ArrayBuffer[Element]()

    while (!remainEquations.isEmpty) {
      val e1 = remainEquations.head
      val condsIsomorphisms = remainEquations.filter { e2 =>
        e1.isIsomorphic(e2)
      }
      val sumValue = condsIsomorphisms.map(_.value).sum
      val newElement = Element(e1.q, e1.rules, e1.stage, sumValue)

      simplifiedElements += newElement

      //      remove the already simplified equations
      remainEquations = remainEquations.diff(condsIsomorphisms)
    }

    Equation(head, simplifiedElements.filter(e => e.value != 0), stage)
  }

  def toNegativeEquation(): Equation = {
    Equation(head.toNegative(), elements.map(_.toNegative()), stage)
  }

  def toEquationWithStage(newStage: Stage): Equation = {
    Equation(head, elements, newStage)
  }

  def toCUEquation(): CUEquation = {
    CUEquation(this, Stage.Decomposed)
  }

  override def toString: String = {
    toStringWithIndent(1)
  }

  override def toStringWithIndent(level: Int) = {
    val str =
      s"Equation(\nhead = ${head}\nstage = ${stage}\natoms = Seq(${elements
        .map(_.toStringWithIndent(1) + "\n\n")
        .reduce(_ + _)})"
    addIndent(level, str)
  }

  override def toViz(): String = {
    val headElement = elements.head
    val equationCaption = elements
      .map { e =>
        e.value match {
          case positiveOne if positiveOne == 1.0  => s"+ (v${e.objID}) "
          case positive if positive > 0           => s"+ (${e.value} * v${e.objID}) "
          case negative if negative < 0           => s"- (${-e.value} * v${e.objID}) "
          case negativeOne if negativeOne == -1.0 => s"- (v${e.objID}) "
        }
      }
      .reduce(_ + _)

    val caption =
      s"""Equation:${headElement.value} * v${headElement.objID} = ${equationCaption}"""
    s"""label="$caption"  """ + elements.map(_.toViz() + "\n \n").reduce(_ + _)
  }
}

object Equation {
  def apply(head: Element, elements: Seq[Element], stage: Stage) =
    new Equation(head, elements, stage)

  def apply(head: Element, element: Element, stage: Stage) =
    new Equation(head, Seq(element), stage)
}

case class SymmetryBreakingRule(lhs: NodeID,
                                rhs: NodeID,
                                isomorphismNodes: Seq[NodeID]) {
  override def toString: String = {
    s"rule:${lhs} < ${rhs}, isomorphismNodes:${isomorphismNodes}"
  }
}

class Element(val q: Query,
              val rules: Seq[SymmetryBreakingRule] = ArrayBuffer(),
              val stage: Stage,
              val value: Double = 1.0)
    extends Query(q.pattern, q.core) {

  override def isIsomorphic(e: IsomorphicAble) = {
    !findIsomorphism(e).isEmpty
  }

  override def findIsomorphism(e: IsomorphicAble) = {
    e match {
      case element: Element => {

        if (element.rules == rules && element.value == value) {
          super.findIsomorphism(element.q)
        } else {
          ArrayBuffer()
        }
      }
      case _ =>
        throw new Exception(
          "cannot compare isomoprhic objects with different class"
        )
    }
  }

  def toNegative() = {
    Element(q, rules, stage, 0 - value)
  }

  def toCUTree(): CUTree = {
    CUTree(this)
  }

  override def toString: String = {
    toStringWithIndent(1)
  }

  override def toStringWithIndent(level: Int) = {
    val str = s"Element(\nquery = ${super.toStringWithIndent(1)}\n" +
      s"symmetryBreakingRules = ${rules.toString()}\nstage = ${stage}\nvalue = ${value.toString})"

    addIndent(level, str)
  }

  override def toViz(): String = {

    val allNodesToIDMap =
      pattern.V().map(id => (id, s"node${objID}${id}")).toMap

    val coreNodesToIDMap = core.V().map(id => (id, s"node${objID}${id}")).toMap
    val coreEdgesOfNodeIDList = core
      .E()
      .map { case (u, v) => (allNodesToIDMap(u), allNodesToIDMap(v)) }
      .filter(p => p._1 != p._2)

    val pNodesToIDMap =
      pattern.V().diff(core.V()).map(id => (id, s"node${objID}${id}")).toMap
    val pEdgesOfNodeIDList = pattern.E().diff(core.E()).map {
      case (u, v) => (allNodesToIDMap(u), allNodesToIDMap(v))
    }

    s"""
       |
       |subgraph cluster_$objID {
       |		label = "Node ID:v_$objID \n Symmetry Breaking Condition: ${rules.isEmpty match {
         case true => "null";
         case false =>
           rules
             .map {
               _.toString
             }
             .reduce(_ + _)
       }}
       Count:$value";
       |		edge [dir=none];
       |
       |    ${coreNodesToIDMap
         .map(id => s"""${id._2}[label="${id._1}",color=red,style=filled];\n""")
         .reduce(_ + _)}
       |    ${pNodesToIDMap
         .map(id => s"""${id._2}[label="${id._1}"];\n""")
         .reduce(_ + _)}
       |		${coreEdgesOfNodeIDList.isEmpty match {
         case false =>
           coreEdgesOfNodeIDList
             .map { case (u, v) => s"$u -> $v[color=red,style=filled];\n" }
             .reduce(_ + _)
         case true => "\n"
       }}
       |    ${pEdgesOfNodeIDList
         .map { case (u, v) => s"$u -> $v;\n" }
         .reduce(_ + _)}
       |}
       |
       |
       |
     """.stripMargin
  }

  override def toEquation(stage: Stage): Equation = {
    throw new NoSuchMethodException()
  }
}

object Element {

  def simplifyRules(e: Element): Element = {
    val usefulConditions = e.rules.filter {
      case SymmetryBreakingRule(u, v, _) => e.containNode(u) && e.containNode(v)
    }
    Element(e.q, usefulConditions, e.stage, e.value, needSimplify = false)
  }

  def apply(q: Query,
            rules: Seq[SymmetryBreakingRule] = ArrayBuffer(),
            stage: Stage = Stage.Unprocessed,
            value: Double = 1.0,
            needSimplify: Boolean = true): Element = {
    if (needSimplify) {
      simplifyRules(new Element(q, rules, stage, value))
    } else {
      new Element(q, rules, stage, value)
    }
  }
}
