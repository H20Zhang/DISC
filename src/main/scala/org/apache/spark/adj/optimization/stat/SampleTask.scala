package org.apache.spark.adj.optimization.stat

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.{Catalog, RelationSchema}
import org.apache.spark.adj.execution.hcube.utils.TriePreConstructor
import org.apache.spark.adj.execution.hcube.{
  HCubeBlock,
  TrieHCubeBlock,
  TupleHCubeBlock
}
import org.apache.spark.adj.execution.subtask.executor.TrieConstructedLeapFrogJoin
import org.apache.spark.adj.execution.subtask.{
  AttributeOrderInfo,
  SubTask,
  TaskInfo,
  TrieConstructedAttributeOrderInfo,
  TrieConstructedLeapFrogJoinSubTask
}
import org.apache.spark.adj.optimization.comp.{
  AttrOrderCostModel,
  OrderComputer
}
import org.apache.spark.adj.optimization.decomposition.graph.Graph
import org.apache.spark.adj.optimization.decomposition.relationGraph.RelationGHDTree
import org.apache.spark.adj.utils.misc.Conf
import org.jgrapht.alg.shortestpath.DijkstraShortestPath
import org.jgrapht.graph.{DefaultEdge, DefaultUndirectedGraph}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

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

//    allSchemas.filter(
//    schema => schema.attrIDs.intersect(prevHyperNodesAttrs).nonEmpty
//  )

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
////TODO: possible deprecated, debug it
//class SampleTask(_shareVector: Array[Int],
//                 _blocks: Seq[HCubeBlock],
//                 sampleTaskInfo: SampleTaskInfo)
//    extends SubTask(_shareVector, _blocks, sampleTaskInfo) {
//
//  private val tasks = sampleTaskInfo.parameterTaskInfos
//  private val schemaToContentMap
//    : mutable.HashMap[RelationSchema, HCubeBlock] = {
//    val theMap = mutable.HashMap[RelationSchema, HCubeBlock]()
//    _blocks.map(f => (f.schema, f)).foreach { f =>
//      theMap(f._1) = f._2
//    }
//    theMap
//  }
//
//  def genSampledParameters(): Seq[SampledParameter] = {
//
//    tasks.foreach { task =>
//      println()
//      println(task)
//    }
//    tasks.map(genOneSampledParameter)
//  }
//
//  private def genOneSampledParameter(
//    sampleParameterTaskInfo: SampleParameterTaskInfo
//  ): SampledParameter = {
//
//    if (sampleParameterTaskInfo.prevHyperNodes.isEmpty) {
//      //prepare test query for the first node
//      var testSchemas = sampleParameterTaskInfo.testQuery
//      val testAttrOrder = sampleParameterTaskInfo.testQueryAttrOrder
//      var testContent = testSchemas
//        .map(schemaToContentMap)
//        .map(_.asInstanceOf[TupleHCubeBlock].content)
//
//      val testBlocks = prepareBlocks(testContent, testSchemas, testAttrOrder)
//      val sampledParameter =
//        performFirstNodeTest(sampleParameterTaskInfo, testBlocks, testAttrOrder)
//
//      sampledParameter
//
//    } else {
//      //prepare sample generation query
//      val sampleSchemas = sampleParameterTaskInfo.sampleQuery
//      val sampleAttrOrder = sampleParameterTaskInfo.sampleQueryAttrOrder
//      val sampleContents = sampleSchemas
//        .map(schemaToContentMap)
//        .map(_.asInstanceOf[TupleHCubeBlock].content)
//
//      val blocks = prepareBlocks(sampleContents, sampleSchemas, sampleAttrOrder)
//      val sampledRelation =
//        prepareSamples(
//          sampleParameterTaskInfo.samplesPerMachine,
//          blocks,
//          sampleAttrOrder,
//          sampleParameterTaskInfo.sampledRelationSchema
//        )
//
//      //prepare test query
//      var testSchemas = sampleParameterTaskInfo.testQuery
//      val testAttrOrder = sampleParameterTaskInfo.testQueryAttrOrder
//      var testContent = testSchemas
//        .map(schemaToContentMap)
//        .map(_.asInstanceOf[TupleHCubeBlock].content)
//
//      val sampleSize = sampledRelation._1.size
//      testSchemas = testSchemas :+ sampledRelation._2
//      testContent = testContent :+ sampledRelation._1
//
//      val testBlocks = prepareBlocks(testContent, testSchemas, testAttrOrder)
//      val sampledParameter =
//        performTest(
//          sampleParameterTaskInfo,
//          testBlocks,
//          testAttrOrder,
//          sampleSize
//        )
//
//      sampledParameter
//    }
//
//  }
//
//  private def prepareBlocks(
//    contents: Seq[Array[Array[Int]]],
//    schemas: Seq[RelationSchema],
//    attrOrder: Array[AttributeID]
//  ): Seq[TrieHCubeBlock] = {
//    contents.zip(schemas).map {
//      case (content, schema) =>
//        val triePreConstructor =
//          new TriePreConstructor(attrOrder, schema, content)
//        val trie = triePreConstructor.construct()
//        TrieHCubeBlock(schema, shareVector, trie)
//    }
//  }
//
//  private def prepareSamples(
//    numOfSamples: Int,
//    blocks: Seq[TrieHCubeBlock],
//    attrOrder: Array[AttributeID],
//    sampledRelationSchema: RelationSchema
//  ): (Array[Array[Int]], RelationSchema) = {
//
//    val info = TrieConstructedAttributeOrderInfo(attrOrder)
//    val sampleSubTask =
//      new TrieConstructedLeapFrogJoinSubTask(shareVector, blocks, info)
//    val sampleSubTaskExecIt =
//      new TimeRecordingTrieConstructedLeapFrogJoin(sampleSubTask, numOfSamples)
//
//    var samples = new ArrayBuffer[Array[Int]]()
//    var i = 0
//    val numOfRawSamplesToCollect = numOfSamples * 10
//    while (i < numOfRawSamplesToCollect && sampleSubTaskExecIt.hasNext) {
//      samples += sampleSubTaskExecIt.next().clone()
//      i += 1
//    }
//
//    Random.setSeed(System.currentTimeMillis())
//    var secondSampleRatio = samples.size.toDouble / numOfSamples
//    samples = samples.filter { f =>
//      Random.nextDouble() < secondSampleRatio
//    }
//
//    //construct content of the sampled relation
//    val sampledRelationAttrIdPos =
//      sampledRelationSchema.attrIDs.map(attrOrder.indexOf).toArray
//    val content =
//      samples.toArray.map(sample => sampledRelationAttrIdPos.map(sample))
//
//    (content, sampledRelationSchema)
//  }
//
//  private def performTest(sampleParameterTaskInfo: SampleParameterTaskInfo,
//                          blocks: Seq[TrieHCubeBlock],
//                          attrOrder: Array[AttributeID],
//                          sampleSize: Int): SampledParameter = {
//    val info = TrieConstructedAttributeOrderInfo(attrOrder)
//    val sampleSubTask =
//      new TrieConstructedLeapFrogJoinSubTask(shareVector, blocks, info)
//    val sampleSubTaskExecIt =
//      new TimeRecordingTrieConstructedLeapFrogJoin(sampleSubTask, 0, false)
//    val sampledInfo = sampleSubTaskExecIt.recordCardinalityAndTime(sampleSize)
//
//    SampledParameter(
//      sampleParameterTaskInfo.prevHyperNodes,
//      sampleParameterTaskInfo.curHyperNodes,
//      sampleParameterTaskInfo.ghd,
//      sampleParameterTaskInfo.samplesPerMachine,
//      sampledInfo._1,
//      sampledInfo._2,
//      0
//    )
//  }
//
//  private def performFirstNodeTest(
//    sampleParameterTaskInfo: SampleParameterTaskInfo,
//    blocks: Seq[TrieHCubeBlock],
//    attrOrder: Array[AttributeID]
//  ): SampledParameter = {
//    val info = TrieConstructedAttributeOrderInfo(attrOrder)
//    val sampleSubTask =
//      new TrieConstructedLeapFrogJoinSubTask(shareVector, blocks, info)
//    val sampleSubTaskExecIt =
//      new TimeRecordingTrieConstructedLeapFrogJoin(
//        sampleSubTask,
//        sampleParameterTaskInfo.samplesPerMachine
//      )
//    val sampledInfo = sampleSubTaskExecIt.recordCardinalityAndTime(0)
//    val firstItSize = sampleSubTaskExecIt.firstItSize
//
//    SampledParameter(
//      sampleParameterTaskInfo.prevHyperNodes,
//      sampleParameterTaskInfo.curHyperNodes,
//      sampleParameterTaskInfo.ghd,
//      sampleParameterTaskInfo.samplesPerMachine,
//      sampledInfo._1,
//      sampledInfo._2,
//      firstItSize
//    )
//  }
//
//}
