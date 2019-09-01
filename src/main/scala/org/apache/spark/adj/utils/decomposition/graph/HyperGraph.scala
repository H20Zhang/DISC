package org.apache.spark.adj.utils.decomposition.graph

import org.apache.spark.adj.utils.decomposition.graph.Graph._
import org.apache.spark.adj.utils.decomposition.graph.HyperNodeGraph.{
  NHyperEdge,
  NHyperEdgeSet,
  NHyperNode,
  NHyperNodeSet
}

import scala.collection.mutable.ArrayBuffer

class HyperNodeGraph(HV: NHyperNodeSet, HE: NHyperEdgeSet) {}

object HyperNodeGraph {
  type NHyperNodeSet = Seq[NHyperNode]
  type NHyperEdgeSet = Seq[NHyperEdge]

  //  HyperNodes that are isomoprhic are given the same id
  case class NHyperNode(val g: Graph) extends VisualizeAble {
    lazy val patternID = GraphCatlog.getcatlog().addToCatlog(g)

    //    Construct induced hyper-node according to the nodeset of current hypernode
    def toInducedHyperNode(edges: EdgeList): NHyperNode = {
      val V = g.V()
      val inducedE = edges.filter {
        case (u, v) => V.contains(u) && V.contains(v)
      }
      val newG = GraphBuilder.newGraph(V, inducedE)
      NHyperNode(newG)
    }

    override def toString: String = {
      s"ID:h${g.objID} G:${g.toString}"
    }

    override def toViz(): String = {
      g.toViz()
    }

    override def toStringWithIndent(level: NodeID): String = ???
  }

  case class NHyperEdge(val u: NHyperNode, val v: NHyperNode) {
    override def toString: String = {
      s"h${u.g.objID}-h${v.g.objID};"
    }
  }
}

// We regard GHD as a special kinds of hypertree
case class NHyperTree(val HV: NHyperNodeSet, val HE: NHyperEdgeSet)
    extends HyperNodeGraph(HV, HE)
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

  def addHyperNode(hyperNode: NHyperNode): Option[NHyperTree] = {
    isEmpty() match {
      case true => {
        Some(NHyperTree(HV :+ hyperNode, HE))
      }
      case false => {
        val potentialHyperEdges = HV
          .filter { node1 =>
            node1.g.containAnyNodes(hyperNode.g.V())
          }
          .map(node => NHyperEdge(node, hyperNode))

        val hypertreeList = potentialHyperEdges
        //          ArrayUtil
        //          .powerset(potentialHyperEdges)
          .map { hyperedge =>
            NHyperTree(HV :+ hyperNode, HE :+ hyperedge)
          }

        val validHyperTreeList = hypertreeList.filter { hypertree =>
          hypertree.isGHD()
        }

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
        case NHyperEdge(u, v) =>
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

  override def toStringWithIndent(level: NodeID): String = ???
}
