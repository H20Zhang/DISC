package org.apache.spark.adj.optimization.stat

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.execution.subtask.TaskInfo
import org.apache.spark.adj.optimization.costBased.comp.{
  AttrOrderCostModel,
  OrderComputer
}
import org.apache.spark.adj.optimization.costBased.decomposition.relationGraph.RelationGHDTree
import org.apache.spark.adj.utils.misc.Conf
import org.jgrapht.alg.shortestpath.DijkstraShortestPath
import org.jgrapht.graph.{DefaultEdge, DefaultUndirectedGraph}

case class SampleTaskInfo(parameterTaskInfos: Seq[SampleParameterTaskInfo])
    extends TaskInfo

case class SampleParameterTaskInfo(prevHyperNodes: Set[Int],
                                   curHyperNodes: Int,
                                   ghd: RelationGHDTree,
                                   totalSamples: Int =
                                     Conf.defaultConf().defaultNumSamples) {

  //common parameters
  val allSchemas = ghd.schemas
  val prevHyperNodesAttrs = prevHyperNodes.toSeq
    .flatMap(ghd.getSchemas)
    .distinct
    .flatMap(_.attrIDs)
    .distinct
  val prevHyperNodesSchemas = prevHyperNodes.toSeq
    .flatMap(ghd.getSchemas)
    .distinct

  val curHyperNodeAttrs =
    ghd.getSchemas(curHyperNodes).flatMap(_.attrIDs).distinct
  val curHyperNodeSchemas =
    ghd.getSchemas(curHyperNodes)

  val commonAttrIDs = prevHyperNodesAttrs.intersect(curHyperNodeAttrs)

  //gen sample related parameters
  val sampleQuery: Seq[RelationSchema] = prevHyperNodesSchemas
  val sampleQueryAttrOrder: Array[AttributeID] = {

    val nodeToPreserve = prevHyperNodes.toSeq :+ curHyperNodes
    val allTraversalOrder = ghd.allTraversalOrder

    //obtain subset of the traversal order that contains the nodeToPreserve
    var subSetOfTraversalOrder = allTraversalOrder.map(
      traversalOrder =>
        traversalOrder.filter(nodeId => nodeToPreserve.contains(nodeId))
    )

    //curHyperNode must be the last node
    subSetOfTraversalOrder = subSetOfTraversalOrder.filter(
      traversalOrder => traversalOrder.last == curHyperNodes
    )

    //compute the distance between the head of the traversalOrder and the last of the traversalOrder
    //, and select the one with maximal minimal distance.
    val nodes = nodeToPreserve
    val edges = ghd.E
      .filter {
        case (u, v) => nodes.contains(u) && nodes.contains(v)
      }
      .map { f =>
        if (f._1 < f._2) {
          f
        } else {
          f.swap
        }
      }
      .distinct

    val g = new DefaultUndirectedGraph[Int, DefaultEdge](classOf[DefaultEdge])

    nodes.foreach(f => g.addVertex(f))
    edges.foreach(f => g.addEdge(f._1, f._2))

    val selectedTraversalOrder = subSetOfTraversalOrder
      .map { traversalOrder =>
        val head = traversalOrder.head
        val tail = traversalOrder.last
        (
          traversalOrder,
          -DijkstraShortestPath.findPathBetween(g, head, tail).getLength
        )
      }
      .sortBy(_._2)
      .head
      ._1
      .dropRight(1)

    val compatibleAttrOrders = ghd.compatibleAttrOrder(selectedTraversalOrder)

    if (compatibleAttrOrders.isEmpty) {
      Array()
    } else {
      compatibleAttrOrders
        .map { attrIdOrder =>
          (
            attrIdOrder,
            AttrOrderCostModel(attrIdOrder, prevHyperNodesSchemas).cost()
          )
        }
        .sortBy(_._2)
        .head
        ._1
    }
  }

  //sampled Relations
  var sampledRelationSchema: RelationSchema = {
    RelationSchema.tempSchemaWithAttrIds(commonAttrIDs.toArray)
  }

  //gen test related Parameters
  val testQuery: Seq[RelationSchema] = {
    if (prevHyperNodes.isEmpty) {
      curHyperNodeSchemas.filter { schema =>
        schema.attrIDs.diff(prevHyperNodesAttrs).nonEmpty
      }
    } else {
      curHyperNodeSchemas.filter { schema =>
        schema.attrIDs.diff(prevHyperNodesAttrs).nonEmpty
      } :+ sampledRelationSchema
    }
  }

  val testQueryAttrOrder: Array[AttributeID] = {
    val headAttrs = commonAttrIDs
    val orderComputer = new OrderComputer(curHyperNodeSchemas)
    val validOrder = orderComputer
      .genAllOrderWithCost()
      .filter {
        case (order, cost) =>
          order.slice(0, headAttrs.size).toSeq == headAttrs
      }
      .sortBy(_._2)

    validOrder.head._1
  }
}

case class SampledParameter(prevHyperNodes: Set[Int],
                            curHyperNodes: Int,
                            ghd: RelationGHDTree,
                            samplesPerMachine: Int,
                            var cardinalityValue: Double,
                            var timeValue: Double)
