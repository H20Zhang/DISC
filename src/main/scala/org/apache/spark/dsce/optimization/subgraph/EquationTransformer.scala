package org.apache.spark.dsce.optimization.subgraph

import org.apache.spark.adj.optimization.costBased.decomposition.graph.Graph.NodeID
import org.apache.spark.dsce.optimization.subgraph
import org.apache.spark.dsce.optimization.subgraph.Element.State
import org.apache.spark.dsce.util.Fraction

import scala.collection.mutable.ArrayBuffer

class EquationTransformer {

  val rules = ArrayBuffer[SubgraphCountRule]()

  def addRule(rule: SubgraphCountRule): Unit = {
    rules += rule
  }

  def applyAllRules(eq: Equation): Equation = {
    var appliedEq = eq
    rules.foreach { rule =>
      appliedEq = appliedEq.transformWithRule(rule)
    }

    appliedEq
  }

  def applyAllRulesTillFix(eq: Equation): Equation = {
    var appliedEq = eq
    var isChanged = true
    var count = Int.MaxValue
    println(s"equation:${eq}")

    while (isChanged && count > 0) {
      val oldEq = appliedEq
      appliedEq = applyAllRules(oldEq)
      if (appliedEq != oldEq) {
        isChanged = true
      } else {
        isChanged = false
      }

      count -= 1

      println(s"appliedEq:${appliedEq}")
    }

    appliedEq
  }

}

abstract class SubgraphCountRule {
  def isMatch(elem: Element): Boolean
  def transform(elem: Element): Seq[Element]
}

class SymmetryBreakRule extends SubgraphCountRule {
  override def isMatch(elem: Element): Boolean =
    elem.mode == State.InducedWithSymmetryBreaked

  override def transform(elem: Element): Seq[Element] = {
    val automorphism = elem.findAutomorphism()
    val transformedElement = subgraph.Element(
      elem.V,
      elem.E,
      elem.C,
      elem.factor * Fraction(1, automorphism.size),
      State.Induced
    )
    Seq(transformedElement)
  }
}

class InducedToNonInduceRule extends SubgraphCountRule {
  override def isMatch(elem: Element): Boolean =
    elem.mode == State.Induced

  override def transform(elem: Element): Seq[Element] = {

    val V = elem.V

    //represent edges of both direction in graph using orderedEdge (srcId < dstId)
    val orderedEdges = elem.E.map { edge =>
      if (edge._1 > edge._2) {
        edge.swap
      } else {
        edge
      }
    }.distinct

    val orderedCliqueEdges = V
      .combinations(2)
      .map(f => (f(0), f(1)))
      .map { edge =>
        if (edge._1 > edge._2) {
          edge.swap
        } else {
          edge
        }
      }
      .toSeq
      .distinct

    val orderedDiffEdges = orderedCliqueEdges.diff(orderedEdges)

    val elements = ArrayBuffer[Element]()
    elements += subgraph.Element(
      elem.V,
      elem.E,
      elem.C,
      elem.factor,
      State.Isomorphism
    )

    for (i <- 1 to orderedDiffEdges.size) {
      //      find the combinations of edges to add
      val edgesCombs = orderedDiffEdges.combinations(i)
      edgesCombs.foreach { edges =>
        val newE = edges.flatMap(f => Iterable(f, f.swap)) ++ elem.E
        elements += subgraph.Element(
          elem.V,
          newE,
          elem.C,
          elem.factor * Fraction(-1, 1),
          State.Induced
        )
      }
    }

    elements
  }
}

class NonInduceToPartialRule extends SubgraphCountRule {
  override def isMatch(elem: Element): Boolean =
    elem.mode == State.Isomorphism

  override def transform(elem: Element): Seq[Element] = {

    val V = elem.V
    val E = elem.E
    val C = elem.C

    var nodeCollapseSets = Seq[Seq[Seq[NodeID]]]()
    nodeCollapseSets = Seq(Seq(Seq(V(0))))

    var j = 1
    while (j < V.size) {
      nodeCollapseSets = nodeCollapseSets.flatMap { nodeCollapseSet =>
        val numSet = nodeCollapseSet.size
        val curNodeId = V(j)

        val newColorCollapseSet = ArrayBuffer[Seq[Int]]()
        nodeCollapseSet.foreach(
          sameColorSet => newColorCollapseSet += sameColorSet
        )
        newColorCollapseSet += Seq(curNodeId)

        val newNodeCollapseSets = newColorCollapseSet +: Range(0, numSet)
          .map { setId =>
            val newNodeCollapseSet = ArrayBuffer[Seq[Int]]()
            nodeCollapseSet.foreach(
              sameColorSet => newNodeCollapseSet += sameColorSet
            )

            newNodeCollapseSet(setId) = nodeCollapseSet(setId) :+ curNodeId
            newNodeCollapseSet
          }
          .filter { nodeCollapseSet =>
            nodeCollapseSet.forall { sameColorSet =>
              sameColorSet
                .combinations(2)
                .map(f => (f(0), f(1)))
                .forall(edge => !E.contains(edge))
            }
          }

        newNodeCollapseSets
      }

      j += 1
    }

    nodeCollapseSets = nodeCollapseSets.filter { nodeCollapseSet =>
      nodeCollapseSet.size != V.size
    }

    println(s"nodeCollapseSets:${nodeCollapseSets}")

    val nodeCollapseMaps = nodeCollapseSets.map { nodeCollapseSet =>
      nodeCollapseSet.flatMap { sameColorSet =>
        if (sameColorSet.intersect(C).nonEmpty) {
          sameColorSet.map(nodeId => (nodeId, sameColorSet.intersect(C).head))
        } else {
          sameColorSet.map(nodeId => (nodeId, sameColorSet.head))
        }
      }.toMap
    }

//    val numColor = V.size
//
//    val posToNodeIdMap = V.zipWithIndex.map(_.swap).toMap
//
//    //compute all color arrangment for all pos
//    val arity = numColor
//    var posColors = Seq[Seq[Int]]()
//    posColors = Range(0, numColor).toSeq.map(f => Seq(f))
//
//    var i = 1
//    while (i < arity) {
//      posColors = posColors.flatMap { posColor =>
//        Range(0, numColor).map(color => posColor :+ color)
//      }
//      i += 1
//    }
//
//    //compute colorMap for all attributes
//    var colorMaps = posColors.map { posColor =>
//      val colorMap = posColor.zipWithIndex
//        .groupBy(_._1)
//        .map(f => (f._1, f._2.map(_._2).map(posToNodeIdMap)))
//        .toMap
//      colorMap
//    }.distinct
//
//    //filter some colorMaps as two node of same cannot have same color
//    colorMaps = colorMaps.filter { colorMap =>
//      colorMap.forall {
//        case (color, nodeIds) =>
//          nodeIds
//            .combinations(2)
//            .map(f => (f(0), f(1)))
//            .forall(edge => !E.contains(edge))
//      }
//    }
//
//
//
//    val nodeCollapseMaps = colorMaps.map { colorMap =>
//      val nodeCollapseMap = colorMap.toSeq.flatMap {
//        case (color, nodeIds) =>
//          var valueNodeId = 0
//          if (nodeIds.intersect(elem.C).nonEmpty) {
//            valueNodeId = nodeIds.intersect(elem.C).head
//          } else {
//            nodeIds.head
//          }
//
//          nodeIds.map(nodeId => (nodeId, valueNodeId))
//      }.toMap
//
//      nodeCollapseMap
//    }

    //gen new elements
    val elements = ArrayBuffer[Element]()
    elements += subgraph.Element(
      elem.V,
      elem.E,
      elem.C,
      elem.factor,
      State.Partial
    )

    nodeCollapseMaps.foreach { nodeCollapseMap =>
      val collapsedV =
        elem.V.map(nodeId => nodeCollapseMap(nodeId)).distinct
      val collapsedE =
        E.map(f => (nodeCollapseMap(f._1), nodeCollapseMap(f._2))).distinct
      val collapsedC =
        elem.C.map(nodeId => nodeCollapseMap(nodeId)).distinct
      elements += subgraph.Element(
        collapsedV,
        collapsedE,
        collapsedC,
        elem.factor * Fraction(-1, 1),
        State.Isomorphism
      )
    }

    elements
  }
}

class CliqueOptimizeRule extends SubgraphCountRule {
  override def isMatch(elem: Element): Boolean = {
    elem.mode == State.Partial && elem.isClique()
  }

  override def transform(elem: Element): Seq[Element] = {
    //gen new elements
    val numAutomorphism = elem.findAutomorphism().size
    val elements = ArrayBuffer[Element]()
    elements += subgraph.Element(
      elem.V,
      elem.E,
      elem.C,
      elem.factor * Fraction(numAutomorphism, 1),
      State.CliqueWithSymmetryBreaked
    )
  }
}
