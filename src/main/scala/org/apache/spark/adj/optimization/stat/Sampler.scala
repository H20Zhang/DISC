package org.apache.spark.adj.optimization.stat

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.{Relation, RelationSchema}
import org.apache.spark.adj.execution.subtask.TaskInfo
import org.apache.spark.adj.optimization.decomposition.relationGraph.RelationGHDTree

//TODO: finish this
class Sampler(Relations: Seq[Relation],
              sampleInfo: SampleTaskInfo,
              databaseScaleRatio: Double = 0.2) {

  private var sampledRelations: Seq[Relation] = ???
  private def prepareSampledDataBase(): Seq[Relation] = ???
  private def performSamplesTask(): Seq[Seq[SampledParameter]] = ???
  private def scaleTheSampleParameters(
    sampledParameters: Seq[Seq[SampledParameter]]
  ): Seq[SampledParameter] = ???

  //perform the sampling and generate the Seq[SampledParameter].
  def genSampledParameters(): Seq[SampledParameter] = ???
}
