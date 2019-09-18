package org.apache.spark.adj.optimization.stat

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.execution.hcube.{HCubeBlock, TupleHCubeBlock}
import org.apache.spark.adj.execution.subtask.{
  AttributeOrderInfo,
  SubTask,
  TaskInfo
}
import org.apache.spark.adj.optimization.decomposition.relationGraph.RelationGHDTree

import scala.collection.mutable

//TODO: finish it
class SampleTask(_shareVector: Array[Int],
                 _blocks: Seq[HCubeBlock],
                 sampleTaskInfo: SampleTaskInfo)
    extends SubTask(_shareVector, _blocks, sampleTaskInfo) {

  private val tasks = sampleTaskInfo.parameterTaskInfos
  private var schemaToContentMap
    : mutable.HashMap[RelationSchema, HCubeBlock] = {
    val theMap = mutable.HashMap[RelationSchema, HCubeBlock]()
    _blocks.map(f => (f.schema, f)).foreach { f =>
      theMap(f._1) = f._2
    }
    theMap
  }

  def genSampledParameters(): Seq[SampledParameter] = {
    tasks.map(genOneSampledParameter)
  }

  private def genOneSampledParameter(
    task: SampleParameterTaskInfo
  ): SampledParameter =
    ???
  private def prepareBlocks(
    task: SampleParameterTaskInfo
  ): Seq[TupleHCubeBlock] = ???
  private def prepareSamples(task: SampleParameterTaskInfo): TupleHCubeBlock =
    ???
  private def testSampleQuery(task: SampleParameterTaskInfo): SampledParameter =
    ???

}

case class SampleTaskInfo(parameterTaskInfos: Seq[SampleParameterTaskInfo])
    extends TaskInfo

case class SampleParameterTaskInfo(prevHyperNodes: Set[Int],
                                   curHyperNodes: Int,
                                   ghd: RelationGHDTree,
                                   samplesPerMachine: Int = 10000) {
  lazy val sampleQuery: Seq[RelationSchema] = ???
  lazy val sampleQueryAttrOrder: Array[AttributeID] = ???
  lazy val sampledRelationAttrs: Array[AttributeID] = ???
  lazy val sampledRelationSchema: RelationSchema = ???

  lazy val testQuery: Seq[RelationSchema] = ???
  lazy val testQueryAttrOrder: Array[AttributeID] = ???
}

case class SampledParameter(prevHyperNodes: Set[Int],
                            curHyperNodes: Int,
                            ghd: RelationGHDTree,
                            samplesPerMachine: Int = 10000,
                            var value: Double)
