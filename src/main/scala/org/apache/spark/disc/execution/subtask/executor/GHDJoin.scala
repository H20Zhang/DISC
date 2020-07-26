package org.apache.spark.disc.execution.subtask.executor

import org.apache.spark.disc.catlog.Catalog.DataType
import org.apache.spark.disc.catlog.{Catalog, Schema}
import org.apache.spark.disc.execution.hcube
import org.apache.spark.disc.execution.hcube.TupleHCubeBlock
import org.apache.spark.disc.execution.subtask.{
  AttributeOrderInfo,
  GHDJoinSubTask,
  LeapFrogJoinSubTask
}
import org.apache.spark.disc.optimization.cost_based.decomposition.relationGraph.RelationGHDTree

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//TODO: Debug
class GHDJoin(task: GHDJoinSubTask) extends LongSizeIterator[Array[DataType]] {

  val schemas = task.blocks.map(_.schema)
  val contents = task.blocks.map(_.content)
  val attrIdToContent =
    schemas.zip(contents).map(f => (f._1.id.get, f._2)).toMap
  val ghd = task.info.ghd

  //the schemas and contents of intermediate relations
  val idMSchemas = mutable.HashMap[Int, Schema]()
  var idMContents = mutable.HashMap[Int, Array[Array[DataType]]]()

  var leapfrogIt: LeapFrogJoin = null

  def init() = {
    preMaterialize()
    yannakakis()
    finalAssemble()
  }

  def preMaterialize() = {
    val idBagSchemas = ghd.V
    val idBagContents = idBagSchemas.map(
      f => (f._1, f._2.map(schema => attrIdToContent(schema.id.get)))
    )

    idBagSchemas.zip(idBagContents).foreach {
      case (idBagSchema, idBagContent) =>
        val id = idBagSchema._1
        val schemas = idBagSchema._2
        val contents = idBagContent._2

        val attrOrder = schemas.flatMap(_.attrIDs).distinct
        val attrOrderInfo = AttributeOrderInfo(attrOrder.toArray)
        val tupleHCubeBlocks = schemas.zip(contents).map {
          case (schema, content) =>
            hcube.TupleHCubeBlock(schema, null, content)
        }

        val leapFrogTask = new LeapFrogJoinSubTask(
          task.shareVector,
          tupleHCubeBlocks,
          attrOrderInfo
        )

        val catalog = Catalog.defaultCatalog()
        val mAttrs = attrOrder.map(id => catalog.getAttribute(id))
        val mSchema = Schema("temp", mAttrs)
        val mContent = leapFrogTask.execute().toArray

        idMSchemas(id) = mSchema
        idMContents(id) = mContent
    }
  }

  def yannakakis() = {
    val yannakakisTask = new YannakakisTask(ghd, idMSchemas, idMContents)
    val reducedIdMContents = yannakakisTask.execute()
    idMContents = reducedIdMContents
  }

  def finalAssemble() = {
    val attrOrder = ghd.compatibleAttrOrder(ghd.allTraversalOrder.head).head
    val attributeOrderInfo = AttributeOrderInfo(attrOrder)
    val tupleHCubeBlocks = idMContents.toArray.map {
      case (id, content) =>
        TupleHCubeBlock(idMSchemas(id), task.shareVector, content)
    }

    val leapFrogTask = new LeapFrogJoinSubTask(
      task.shareVector,
      tupleHCubeBlocks,
      attributeOrderInfo
    )

    leapfrogIt = leapFrogTask.execute()
  }

  override def hasNext: Boolean = leapfrogIt.hasNext
  override def next(): Array[DataType] = leapfrogIt.next()

  override def longSize: Long = {
    size
  }
}

class YannakakisTask(
  ghd: RelationGHDTree,
  idMSchemas: mutable.HashMap[Int, Schema],
  idMContents: mutable.HashMap[Int, Array[Array[DataType]]]
) {

  var rootId: Int = Int.MaxValue
  val bottomUpPass = ArrayBuffer[(Int, Int)]()
  val topBottomPass = ArrayBuffer[(Int, Int)]()
  val reducedIdMContents = mutable.HashMap[Int, Array[Array[DataType]]]()

  //determine the root, how relations perform bottomUpPass semi-joins and topBottomPass semi-joins, and initialize reducedIdMContents
  def init() = {
    //bfs over GHD
    val root = ghd.V.head._1
    var level = 0
    var nextLevel = ArrayBuffer[Int]()
    val visited = ArrayBuffer[Int]()

    //record how nodes of each level are connected to the next level
    val levelMap = mutable.HashMap[Int, Array[(Int, Int)]]()

    val adjList = ghd.E
      .flatMap(f => Array(f, f.swap))
      .groupBy(_._1)
      .map(f => (f._1, f._2.map(g => g._2)))
      .toMap

    nextLevel += root
    while (nextLevel.nonEmpty) {

      val prevLevel = nextLevel.toArray
      nextLevel = ArrayBuffer[Int]()
      val connections = ArrayBuffer[(Int, Int)]()

      prevLevel.foreach { id =>
        val nexts = adjList(id).diff(visited)
        nexts.foreach { nextId =>
          connections += ((id, nextId))
          nextLevel += nextId
        }
      }
      levelMap(level) = connections.toArray

      level += 1

      nextLevel = nextLevel.distinct
    }

    //init topbottomPass
    var i = 0
    while (i < level) {
      topBottomPass ++= levelMap(i)
    }

    //init bottomUpPass
    i = level - 2
    while (i >= 0) {
      bottomUpPass ++= levelMap(i).map(_.swap)
    }

    //init rootId
    rootId = root

    //init reducedIdMContent
    reducedIdMContents ++= idMContents
  }

  //replace the content of relation with dstId by the semi-join result of it and relation with srcId
  def performSemiJoinAndReplace(srcId: Int, dstId: Int): Unit = {
    val srcSchema = idMSchemas(srcId)
    val dstSchema = idMSchemas(dstId)

    val srcContent = reducedIdMContents(srcId)
    val dstContent = reducedIdMContents(dstId)

    val commonAttrIds = srcSchema.attrIDs.intersect(dstSchema.attrIDs)
    val commonAttrIdPosOfSrc =
      commonAttrIds.map(attrId => srcSchema.attrIDs.indexOf(attrId)).toArray
    val commonAttrIdPosOfDst =
      commonAttrIds.map(attrId => dstSchema.attrIDs.indexOf(attrId)).toArray

    val projectedSrcContent = mutable.HashSet[Array[DataType]]()
    srcContent.foreach { tuple =>
      val projectedTuple = commonAttrIdPosOfSrc.map(f => tuple(f))
      projectedSrcContent.add(projectedTuple)
    }

    val reducedDstContent = ArrayBuffer[Array[DataType]]()
    dstContent.foreach { tuple =>
      val projectedTuple = commonAttrIdPosOfDst.map(f => tuple(f))
      if (projectedSrcContent.contains(projectedTuple)) {
        reducedDstContent += tuple
      }
    }

    reducedIdMContents(dstId) = reducedDstContent.toArray
  }

  def execute(): mutable.HashMap[Int, Array[Array[DataType]]] = {

    bottomUpPass.foreach {
      case (srcId, dstId) =>
        performSemiJoinAndReplace(srcId, dstId)
    }

    topBottomPass.foreach({
      case (srcId, dstId) =>
        performSemiJoinAndReplace(srcId, dstId)
    })

    reducedIdMContents
  }
}
