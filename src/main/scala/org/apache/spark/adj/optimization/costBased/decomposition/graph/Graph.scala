package org.apache.spark.adj.optimization.costBased.decomposition.graph

import org.apache.spark.adj.optimization.costBased.decomposition.graph.Graph._
import org.apache.spark.adj.utils.misc.LogAble
import org.jgrapht.alg.connectivity.ConnectivityInspector
import org.jgrapht.graph.{DefaultEdge, DefaultUndirectedGraph}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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

  def toGHDs(): Seq[NHyperTree] = {
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

object Graph {
  type RawGraph = DefaultUndirectedGraph[Int, DefaultEdge]
  type Mapping = Map[NodeID, NodeID]

  // normal graph
  type NodeID = Int
  type Edge = (NodeID, NodeID)
  type NodeList = Seq[NodeID]
  type EdgeList = Seq[Edge]
  type ADJList = mutable.HashMap[NodeID, ArrayBuffer[NodeID]]

  // hypertree, where a hypernode is a graph
  type GraphID = Int

  trait VisualizeAble {
    def toStringWithIndent(level: Int): String
    def addIndent(level: Int, str: String) = {
      str
        .split("\n")
        .map(f => s"\n${Seq.fill(level)("\t").foldLeft("")(_ + _)}$f")
        .reduce(_ + _)
        .drop(1 + level)
    }
    def toViz(): String
    lazy val objID: String = UniID.uniqueID()

    def display() = {}
  }

  object UniID {
    var id = 0
    def uniqueID() = {
      id = id + 1
      id.toString
    }
  }

  trait TestAble {
    def test(): Unit
  }

  trait IsomorphicAble {
    def isIsomorphic(p: IsomorphicAble): Boolean

    def findIsomorphism(p: IsomorphicAble): Seq[Mapping]

    def findIsomorphismUnderConstriant(p: IsomorphicAble,
                                       constraint: Mapping): Seq[Mapping]

    def findAutomorphism(): Seq[Mapping]

    def findAutomorphismUnderConstriant(constraint: Mapping): Seq[Mapping]
  }

}

object GraphBuilder {

  def newGraph(edgeList: String): Graph = {
    val edges = edgeList.split(";").map { f =>
      val edgeString = f.split("-")
      (edgeString(0).toInt, edgeString(1).toInt)
    }

    val nodes = edges.flatMap(f => ArrayBuffer(f._1, f._2))

    val graph: Graph = GraphBuilder.newGraph(nodes, edges)
    graph
  }

  //  node only graph
  def newGraph(V: NodeList, E: EdgeList): Graph = {

    //    assert((!V.isEmpty) || (!E.isEmpty), s"One of the V and E should not be empty. \n V:${V} \n E:${E}")

    val nodes = (V ++ E.flatMap(f => Array(f._1, f._2))).distinct

    val sortedEdges = E.map { f =>
      if (f._1 < f._2) {
        f
      } else {
        f.swap
      }
    }.distinct

    val g = new DefaultUndirectedGraph[Int, DefaultEdge](classOf[DefaultEdge])

    nodes.foreach(f => g.addVertex(f))
    sortedEdges.foreach(f => g.addEdge(f._1, f._2))

    Graph(g)
  }

}
