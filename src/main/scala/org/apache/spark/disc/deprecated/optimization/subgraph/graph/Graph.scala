package org.apache.spark.disc.deprecated.optimization.subgraph.graph

import org.apache.spark.disc._
import org.apache.spark.disc.deprecated.optimization.aggregate.util.{
  HyperTree,
  HyperTreeDecomposer,
  WidthCalculator
}
import org.apache.spark.disc.deprecated.optimization.subgraph.visualization.VisualizeAble
import org.apache.spark.disc.util.GraphBuilder
import org.jgrapht.alg.connectivity.ConnectivityInspector

// pattern that is represented as undirected graph, we assume that nodeID are from 0
case class Graph(g: RawGraph)
    extends VisualizeAble
    with IsomorphicAble
    with LogAble
    with TestAble {

  private val _g = g

  def rawGraph() = {
    _g
  }

  def adj() = {
    GraphUtil.adjacentList(_g)
  }

  def containEdge(e: Edge) = {
    g.containsEdge(e._1, e._2) || g.containsEdge(e._2, e._1)
  }

  override def isIsomorphic(p: IsomorphicAble) = {
    p match {
      case graph: Graph => GraphUtil.isIsomorphic(_g, graph._g)
      case _ =>
        throw new Exception(
          "cannot compare whether two different class is Ismorphic"
        )
    }
  }

  def isEmpty(): Boolean = {
    isNodesEmpty() && isEdgesEmpty()
  }

  def isNodesEmpty(): Boolean = {
    V().isEmpty
  }

  def isEdgesEmpty(): Boolean = {
    E().isEmpty
  }

  def containNode(v: NodeID) = {
    g.containsVertex(v)
  }

  override def findIsomorphism(p: IsomorphicAble) = {
    p match {
      case graph: Graph => GraphUtil.findIsomorphism(_g, graph._g)
      case _ =>
        throw new Exception(
          "cannot compare whether two different class is Ismorphic"
        )
    }
  }

  def containAnyNodes(vs: Seq[NodeID]) = {
    !V().intersect(vs).isEmpty
  }

  override def findIsomorphismUnderConstriant(p: IsomorphicAble,
                                              constraint: Mapping) = {
    p match {
      case graph: Graph =>
        GraphUtil.findIsomorphismUnderConstraint(_g, graph._g, constraint)
      case _ =>
        throw new Exception(
          "cannot compare whether two different class is Ismorphic"
        )
    }
  }

  override def findAutomorphism() = {
    GraphUtil.findAutomorphisms(_g)
  }

  override def findAutomorphismUnderConstriant(constraint: Mapping) = {
    GraphUtil.findIsomorphismUnderConstraint(_g, _g, constraint)
  }

  def rootedSubtree(root: NodeID, subTreeRoot: NodeID) = {
    GraphUtil.findRootedSubTree(root, subTreeRoot, _g)
  }

  def neighborsForNode(nodeID: NodeID): Seq[NodeID] = {
    E()
      .filter { case (u, v) => u == nodeID || v == nodeID }
      .flatMap { case (u, v) => Array(u, v) }
      .distinct
      .filter(n => n != nodeID)
  }

  def isTree() = {

    val isSingleNodeForest = V().size > 1 && E().isEmpty

    (!GraphUtil.containCycles(_g)) && (!isSingleNodeForest)
  }

  def containSubgraph(g: Graph) = {
    g.E().filter { case (u, v) => u != v }.isEmpty match {
      case true  => containNodes(g.V())
      case false => containEdges(g.E())
    }
  }

  def V() = {
    GraphUtil.nodeList(_g)
  }

  def E() = {
    GraphUtil.edgeList(_g)
  }

  def containEdges(es: Seq[Edge]) = {
    es.forall(containEdge(_))
  }

  def containCycle() = {
    GraphUtil.containCycles(_g)
  }

  def isConnected() = {
    val inspector = new ConnectivityInspector(_g)
    inspector.isConnected()
  }

  def containNodes(vs: Seq[NodeID]) = {
    vs.forall(v => containNode(v))
  }

  def toInducedGraph(baseGraph: Graph): Graph = {
    val nodeSet = V()

    val inducedEdges = baseGraph.E().filter {
      case (u, v) =>
        nodeSet.contains(u) && nodeSet.contains(v)
    }

    GraphBuilder.newGraph(nodeSet, inducedEdges)
  }

  def toGHDs(): Seq[HyperTree] = {
    HyperTreeDecomposer.allGHDs(this)
  }

  //  the fractional width of the graph
  def fractionalWidth() = {
    Math.round(WidthCalculator.width(this) * 100D) / 100D
  }

  override def toString: String = {
    val edgelist = E()

    edgelist.isEmpty match {
      case true =>
        s"""V:${V()} E:Empty""".stripMargin
      case false =>
        s"""V:${V()} E:${edgelist
             .map { case (u, v) => s"${u}-${v};" }
             .reduce(_ + _)}""".stripMargin
    }
  }

  override def toViz(): String = {

    val nodesToIDMap = V().map(id => (id, s"node${objID}${id}")).toMap
    val edgesOfNodeIDList = E().map {
      case (u, v) => (nodesToIDMap(u), nodesToIDMap(v))
    }

    s"""
       |subgraph cluster_$objID {
       |		label = "Node ID:v$objID";
       |		edge [dir=none];
       |
       |    ${nodesToIDMap
         .map(id => s"""${id._2}[label="${id._1}"];\n""")
         .reduce(_ + _)}
       |		${edgesOfNodeIDList
         .map { case (u, v) => s"$u -> $v;\n" }
         .reduce(_ + _)}
       |}
       |
       |
       |
     """.stripMargin
  }

  override def test(): Unit = {
    logger.info("start testing inner methods of graph")
  }

  override def toStringWithIndent(level: GraphID): String = ???
}
