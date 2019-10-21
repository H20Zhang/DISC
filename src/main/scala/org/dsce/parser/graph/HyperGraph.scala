package org.dsce.parser.graph

import HyperGraph.{HyperEdgeSet, HyperNodeSet}
import org.dsce.util.GraphBuilder
import org.dsce.{EdgeList, NodeID, org.dsce.parser.graph}
import org.dsce.util.GraphBuilder
import org.dsce.parser.visualization.VisualizeAble

import scala.collection.mutable.ArrayBuffer

//  HyperNodes that are isomoprhic are given the same id
case class HyperNode(val g: Graph) extends VisualizeAble {
  lazy val patternID = GraphCatlog.getcatlog().addToCatlog(g)

  //    Construct induced hyper-node according to the nodeset of current hypernode
  def toInducedHyperNode(edges: EdgeList): HyperNode = {
    val V = g.V()
    val inducedE = edges.filter {
      case (u, v) => V.contains(u) && V.contains(v)
    }
    val newG = GraphBuilder.newGraph(V, inducedE)
    graph.HyperNode(newG)
  }

  override def toString: String = {
    s"ID:h${g.objID} G:${g.toString}"
  }

  override def toViz(): String = {
    g.toViz()
  }

  override def toStringWithIndent(level: NodeID): String = ???
}

case class HyperEdge(val u: HyperNode, val v: HyperNode) {
  override def toString: String = {
    s"h${u.g.objID}-h${v.g.objID};"
  }
}

class HyperGraph {}

object HyperGraph {
  type HyperNodeSet = Seq[HyperNode]
  type HyperEdgeSet = Seq[HyperEdge]
}

// We regard GHD as a special kinds of hypertree
case class HyperTree(val HV: HyperNodeSet, val HE: HyperEdgeSet)
    extends HyperGraph
    with VisualizeAble {

  private lazy val hvToIDMap = HV.zipWithIndex.toMap
  private lazy val idtoHVMap = hvToIDMap.map(_.swap)
  private lazy val he = HE.map(f => (hvToIDMap(f.u), hvToIDMap(f.v)))
  private lazy val h =
    GraphBuilder.newGraph(he.flatMap(f => ArrayBuffer(f._1, f._2)).distinct, he)

  def toGraph(): Graph = {
    val E = HV.flatMap(n => n.g.E()).distinct
    val V = E.flatMap(f => ArrayBuffer(f._1, f._2)).distinct

    GraphBuilder.newGraph(V, E)
  }

  def rootedSubHyperTree(treeRoot: HyperNode, inducedTreeRoot: HyperNode) = {
    val treeRootID = hvToIDMap(treeRoot)
    val inducedTreeRootID = hvToIDMap(inducedTreeRoot)

    val inducedH = h.rootedSubtree(treeRootID, inducedTreeRootID)

    val inducedHV = inducedH.V().map(id => idtoHVMap(id))
    val inducedHE =
      inducedH.E().map { case (u, v) => HyperEdge(idtoHVMap(u), idtoHVMap(v)) }

    HyperTree(inducedHV, inducedHE)
  }

  def neighborsForNode(hyperNode: HyperNode): Seq[HyperNode] = {
    val id = hvToIDMap(hyperNode)
    h.neighborsForNode(id).map(idtoHVMap(_))
  }

  def addHyperNode(hyperNode: HyperNode): Option[HyperTree] = {
    isEmpty() match {
      case true => {
        Some(HyperTree(HV :+ hyperNode, HE))
      }
      case false => {
        val potentialHyperEdges = HV
          .filter { node1 =>
            node1.g.containAnyNodes(hyperNode.g.V())
          }
          .map(node => HyperEdge(node, hyperNode))

        val hypertreeList = potentialHyperEdges
        //          ArrayUtil
        //          .powerset(potentialHyperEdges)
          .map { hyperedge =>
            HyperTree(HV :+ hyperNode, HE :+ hyperedge)
          }

        val validHyperTreeList = hypertreeList.filter { hypertree =>
          hypertree.isGHD()
        //            hypertree.isGHD()
        }

        //        assert(validHyperTreeList.forall(t => t.isGHD()))

        validHyperTreeList.isEmpty match {
          case true  => None
          case false => Some(validHyperTreeList.head)
        }
      }
    }
  }

  def isEmpty(): Boolean = {
    HV.isEmpty && HE.isEmpty
  }

  //    determine whether current hypertree is a GHD
  def isGHD(): Boolean = {
    h.isTree() && satisfiesRunningPathProperty()
  }

  //  Determine whether current GHD satisfies running path property
  def satisfiesRunningPathProperty(): Boolean = {

    //  return the subgraph of the hypertree based on the running path induced subgraph
    def runningPathSubGraph(nodeID: NodeID): Graph = {
      val relevantHyperNodes = HV.filter { hypernode =>
        hypernode.g.containNode(nodeID)
      }

      val relevantEdges = HE.filter {
        case HyperEdge(u, v) =>
          relevantHyperNodes.contains(u) && relevantHyperNodes.contains(v)
      }

      val relevantEdgesOfHyperNodeID =
        relevantEdges.map(f => (hvToIDMap(f.u), hvToIDMap(f.v)))

      val g = GraphBuilder.newGraph(
        relevantHyperNodes.map(f => hvToIDMap(f)),
        relevantEdgesOfHyperNodeID
      )

      g
    }

    val nodeIDset = HV.flatMap(hypernode => hypernode.g.V()).distinct

    nodeIDset.forall { nodeId =>
      runningPathSubGraph(nodeId).isConnected()
    }
  }

  def isIsomorphic(t: HyperTree): Boolean = {

    //    find isomorphism between nodes
    var hyperNodeToMatch = t.HV.toSet

    val existNodeIso = HV
      .map { n1 =>
        val matchForN1 = hyperNodeToMatch
          .map(n2 => (n2, n2.g.isIsomorphic(n1.g)))
          .filter(_._2 == true)
          .map(_._1)
        matchForN1.isEmpty match {
          case true => (n1, None)
          case false =>
            val m = matchForN1.head; hyperNodeToMatch = hyperNodeToMatch - m;
            (n1, Some(m))
        }
      }
      .forall(_._2.nonEmpty)

    h.isIsomorphic(t.h) && existNodeIso
  }

  def fractionalHyperNodeWidth() = {
    HV.map(_.g.fractionalWidth()).max
  }

  def hyperNodeWidth() = {
    HV.map(_.g.E().size).max
  }

  override def toString: String = {
    s"""
       |V:${HV}
       |E:${HE}
     """.stripMargin
  }

  override def toViz(): String = {
    s"""
       |
       |label="${HE.toString()}"
       |${HV.map(_.g.toViz()).reduce(_ + _)}
       |
       |
     """.stripMargin
  }

  //  def isGHDQuickTest(IDs:Seq[NodeID]):Boolean = {
  //    h.isTree() && quickRunningPathPropertyTest(IDs)
  //  }
  //
  //  def quickRunningPathPropertyTest(IDs: Seq[NodeID]):Boolean = {
  //    IDs.forall(nodeId => runningPathSubGraph(nodeId).isConnected())
  //  }
  override def toStringWithIndent(level: NodeID): String = ???
}
