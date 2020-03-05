package org.apache.spark.dsce.optimization.subgraph

import org.apache.spark.dsce.optimization.subgraph.Element.State.{State}
import org.apache.spark.dsce.util.{Fraction, Graph}
import org.apache.spark.dsce.{Edge, Mapping, NodeID}

import scala.collection.mutable.ArrayBuffer

class EQ {}

class Pattern(V: Seq[NodeID], E: Seq[Edge], val C: Seq[NodeID])
    extends Graph(V, E) {

  override def findIsomorphism(p: Graph) = {

    p match {
      case p2: Pattern => {
        val mappings = super.findIsomorphism(p2)

        if (C.toSet == p2.C.toSet) {
          val constraints = C.zip(p2.C).toMap

          //    filter the mappings that violate matchedNodes
//          println(
//            s"V:${V}, E:${E}, C:${C.toList}, V2:${p2.V}, E2:${p2.E}, C2:${p2.C}"
//          )
//          println(s"mapping:${mappings}")
//          println(s"constraints:${constraints}")

          val validMappings = mappings.filter { mapping =>
            C.forall(nodeID => mapping(nodeID) == constraints(nodeID))
          }

          validMappings
        } else {
          Seq()
        }
      }
      case p1: Graph => {
        super.findIsomorphism(p1)
      }
    }
  }

  def toGraph: Graph = {
    new Graph(V, E)
  }

  override def findAutomorphism(): Seq[Mapping] = {
    val automorphism = findIsomorphism(this)
    automorphism
  }

  override def toString: String = {
    s"Pattern(V:${V}, E:${E}, C:${C.toList})"
  }
}

case class Element(override val V: Seq[NodeID],
                   override val E: Seq[Edge],
                   override val C: Seq[NodeID],
                   factor: Fraction,
                   state: State)
    extends Pattern(V, E, C) {}

object Element {
  object State extends Enumeration {
    type State = Value
    val CliqueWithSymmetryBreaked, Partial, NonInduced, Induced,
    InducedWithSymmetryBreaked = Value
  }
}

case class Equation(head: Element, body: Seq[Element]) {

  def simplify(): Equation = {
    val optimizedBody1 = ArrayBuffer[Element]()
    body.foreach { element =>
      var i = 0
      var doesExists = false
      while (i < optimizedBody1.size) {
        val newBodyElement = optimizedBody1(i)
        if (newBodyElement.isIsomorphic(element) && newBodyElement.state == element.state) {
          optimizedBody1(i) = Element(
            newBodyElement.V,
            newBodyElement.E,
            newBodyElement.C,
            newBodyElement.factor + element.factor,
            newBodyElement.state
          )
          doesExists = true
        }
        i += 1
      }

      if (doesExists == false) {
        optimizedBody1 += element
      }
    }

    val optimizedBody2 = ArrayBuffer[Element]()
    optimizedBody1.foreach { element =>
      if (element.factor.toDouble != 0) {
        optimizedBody2 += element
      }
    }

    Equation(head, optimizedBody2)

//    this
  }

  def transformWithRule(rule: SubgraphCountRule) = {

    val matchedIdx = body.indexWhere(p => rule.isMatch(p))

    if (matchedIdx != -1) {
      val beginIdx = 0
      val endIdx = body.size
      Equation(
        head,
        body.slice(beginIdx, matchedIdx) ++ rule
          .transform(body(matchedIdx)) ++ body.slice(matchedIdx + 1, endIdx)
      )
    } else {
      this
    }
  }
}
