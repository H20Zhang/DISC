package org.apache.spark.dsce.deprecated.optimization.aggregate.util

import org.apache.spark.dsce.{EdgeList, LogAble, TestAble}
import org.apache.spark.dsce.deprecated.optimization.aggregate.util
import org.apache.spark.dsce.deprecated.optimization.subgraph.graph.Graph
import org.apache.spark.dsce.util.testing.ExamplePattern
import org.apache.spark.dsce.util.testing.ExamplePattern.PatternName
import org.apache.spark.dsce.util.{ArrayUtil, GraphBuilder}

import scala.collection.mutable.ArrayBuffer

object HyperTreeDecomposer extends TestAble with LogAble {

  def test(): Unit = {

    val g = ExamplePattern.graphWithName(PatternName.squareEdge)
    val edgeSet = g.E()


    val GHDs = allGHDs(g)
    logger.debug(s"${GHDs}")
    //    logger.debug(s"${GHDs.last}")
    //    logger.debug(s"${GHDs.last.isGHD()}")

    val validGHD = GHDs.filter { ghd => ghd.isGHD() }
    logger.debug(s"valid GHD number:${validGHD.size}, all GHD number:${GHDs.size}")

    //    GHDs.last.display()
    //    val invalidGHD = GHDs.diff(validGHD)
    //    logger.debug(s"${invalidGHD.last}")
    //    invalidGHD.last.display()
    //        logger.debug(s"${invalidGHD.last.isGHD()}")
    //        logger.debug(s"${invalidGHD.last.satisfiesRunningPathProperty()}")
    //        logger.debug(invalidGHD.last.runningPathSubGraph(1).isConnected())

    //    logger.debug(s"${validGHD.last}")


    //    validGHD.last.display()
    //    validGHD.last.runningPathSubGraph(1).display()


    //    val lastGHD = GHDs.last
    //    lastGHD.display()
    //    println(s"last GHD is ${lastGHD}")

    //
    //    val runningPathSubgraph0 = lastGHD.runningPathSubGraph(3)
    //    println(runningPathSubgraph0)
    //    println(runningPathSubgraph0.isTree())
    //    println(lastGHD.satisfiesRunningPathProperty())
    //    println(lastGHD.isGHD())


    //    one of the hypertree
    //val g1 = GraphBuilder.newGraph("0-1;1-2")
    //    val g2 = GraphBuilder.newGraph("2-3;0-3")
    //
    //    val n1 = HyperNode(g1)
    //    val n2 = HyperNode(g2)
    //
    //    val t1 = HyperTree(Seq(n1, n2), Seq(HyperEdge(n1,n2)))
    //    t1.display()
    //    println(t1.isGHD())
    //    println(t1.satisfiesRunningPathProperty())
    //    Range(0,4).map(t1.runningPathSubGraph(_)).foreach{g => println(g);println(g.isTree())}


  }

  //  Find all GHD decomposition
  def allGHDs(g: Graph) = {

    var extendableTree = ArrayBuffer[(HyperTree, Seq[(Int, Int)])]()
    val GHDs = ArrayBuffer[HyperTree]()
    extendableTree += ((util.HyperTree(ArrayBuffer(), ArrayBuffer()), g.E()))

    var counter = 0

    while (!extendableTree.isEmpty) {

      counter += 1

      val (hypertree, remainingEdges) = extendableTree.head
      extendableTree = extendableTree.drop(1)

      remainingEdges.isEmpty match {
        case true => GHDs += hypertree
        case false => {
          val newNodes = potentialHyperNodes(g, hypertree, remainingEdges)

          val newTrees = newNodes
            .map { case (node, edges) => (hypertree.addHyperNode(node), edges) }
            .filter(_._1.nonEmpty).map(f => (f._1.get, f._2))

          newTrees.foreach(nodeAndedges => extendableTree += nodeAndedges)

        }
      }

      //      println(s"extendableTree: ${extendableTree}")
    }

    logger.debug(s"counter:${counter}")

    GHDs
  }

  //  Find the hyper-nodes, the graph inside a hyper-node must be connected and node induced graph
  private def potentialHyperNodes(basedGraph: Graph, hypertree: HyperTree, remainEdges: EdgeList): Seq[(HyperNode, EdgeList)] = {

    val potentialEdgeSets = ArrayUtil.powerset(remainEdges)

    //    filter the edgeSet that result in disconnected graph
    var potentialGraphs = potentialEdgeSets
      .map(edgeSet => GraphBuilder.newGraph(edgeSet.flatMap(f => ArrayBuffer(f._1, f._2)).distinct, edgeSet))

    //    convert the graph into induced graph
    potentialGraphs = potentialGraphs
      .map(graph => graph.toInducedGraph(basedGraph).E())
      .distinct
      .map(edgeList => GraphBuilder.newGraph(edgeList.flatMap(f => ArrayBuffer(f._1, f._2)).distinct, edgeList))

    //    previous node in hypertree must not be subgraph of new node
    potentialGraphs = potentialGraphs
      .filter(g => !hypertree.HV.exists(n => g.containSubgraph(n.g) || n.g.containSubgraph(g)))



    val validGraphs = potentialGraphs.filter {
      _.isConnected()
    }

    //        logger.debug(validGraphs.foreach(println(_)))

    hypertree.isEmpty() match {
      case true => {
        validGraphs.map(g => (util.HyperNode(g), remainEdges.diff(g.E()))).filter(_._1.g.containEdge(remainEdges.head))
      }
      case false => {
        validGraphs.map(g => (util.HyperNode(g), remainEdges.diff(g.E())))
          .filter(_._1.g.containEdge(remainEdges.head))
          .filter {
            case (hypernode, remainingEdge) =>
              hypertree.HV.exists(hypernode1 => hypernode.g.containAnyNodes(hypernode1.g.V()))
          }
      }
    }



    //    logger.debug(potentialHyperNodes)
    //    logger.debug(hypertree)
    //
    //    potentialHyperNodes

  }


}