package org.apache.spark.adj.optimization.stat

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.{Catalog, Relation, RelationSchema}
import org.apache.spark.adj.execution.hcube.pull.{HCubePlan, PullHCube}
import org.apache.spark.adj.execution.subtask.{
  SubTask,
  SubTaskFactory,
  TaskInfo
}
import org.apache.spark.adj.optimization.comp.{EnumShareComputer, OrderComputer}
import org.apache.spark.adj.optimization.decomposition.relationGraph.RelationGHDTree
import org.apache.spark.adj.utils.misc.Conf
import org.apache.spark.rdd.RDD

import scala.util.Random

//TODO: debug
class Sampler(relations: Seq[Relation],
              sampleInfo: SampleTaskInfo,
              databaseScaleRatio: Double = 0.2) {

  private val sampledRelations: Seq[Relation] = prepareSampledDataBase()
  private var share: Map[AttributeID, Int] = prepareShare()

  def prepareShare(): Map[AttributeID, Int] = {

    val schemas = sampledRelations.map(_.schema)
    val statistic = Statistic.defaultStatistic()
    sampledRelations.foreach(statistic.add)

    val shareComputer =
      new EnumShareComputer(schemas, Conf.defaultConf().taskNum)
    share = shareComputer.optimalShare()._1

    share
  }

  private def prepareSampledDataBase(): Seq[Relation] = {
    val rdds = relations.map(_.rdd)
    val catalog = Catalog.defaultCatalog()
    val schemas = relations.map(_.schema)
    val newSchemas = schemas.map { schema =>
      val attrs = schema.attrs
      val name = s"Sampled${schema.name}"
      val sampledRelationSchema = RelationSchema(name, attrs)
      sampledRelationSchema.register()

      sampledRelationSchema
    }

    val sampledRDDs = rdds.map { rdd =>
      val sampledRDD = rdd.mapPartitions { it =>
        Random.setSeed(System.nanoTime())
        it.filter { p =>
          Random.nextDouble() < databaseScaleRatio
        }
      }

      val output = sampledRDD.cache()
      sampledRDD.count()

      output
    }

    newSchemas.zip(sampledRDDs).map {
      case (schema, rdd) =>
        catalog.setContent(schema, rdd)
        Relation(schema, rdd)
    }
  }

  private def performSamplesTask(): Seq[Seq[SampledParameter]] = {

    val hcubePlan = HCubePlan(sampledRelations, share)
    val hcube = new PullHCube(hcubePlan, sampleInfo)
    val subTaskRDD = hcube.genHCubeRDD()

    subTaskRDD
      .map { task =>
        val subSampleTask =
          new SampleTask(
            task.shareVector,
            task.blocks,
            task.info.asInstanceOf[SampleTaskInfo]
          )

        subSampleTask.genSampledParameters()
      }
      .collect()
  }

  private def scaleTheSampleParameters(
    sampledParameters: Seq[Seq[SampledParameter]]
  ): Seq[SampledParameter] = {
    val summedParameters = sampledParameters
      .flatMap(f => f)
      .map { f =>
        ((f.prevHyperNodes, f.curHyperNodes), f)
      }
      .groupBy(_._1)
      .map {
        case (key, parameters) =>
          val headParameter = parameters.head._2
          val summedValue = parameters.map { parameter =>
            parameter._2.value
          }.sum

          headParameter.value = summedValue
          headParameter
      }
      .toSeq

    val scaledParameters = summedParameters.map { parameter =>
      val ghd = parameter.ghd
      val nodeIdToRelations = ghd.V.toMap
      val coreAttrIds = parameter.prevHyperNodes
        .map(nodeIdToRelations)
        .flatMap(f => f)
        .flatMap(_.attrIDs)
        .toSeq
      val leafAttrIds =
        nodeIdToRelations(parameter.curHyperNodes).flatMap(_.attrIDs).distinct
      val diffAttrIds = leafAttrIds.diff(coreAttrIds)
      val shareForDiffAttrIds = diffAttrIds.map(share)
      val scaleFactor = shareForDiffAttrIds.product
      parameter.value = parameter.value * scaleFactor
      parameter
    }

    scaledParameters
  }

  //perform the sampling and generate the Seq[SampledParameter].
  def genSampledParameters(): Seq[SampledParameter] = {
    val unScaledSampledParameters = performSamplesTask()
    val sampledParameters = scaleTheSampleParameters(unScaledSampledParameters)
    sampledParameters
  }
}
