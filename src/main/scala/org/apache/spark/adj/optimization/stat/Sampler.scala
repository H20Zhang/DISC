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
import org.apache.spark.adj.plan.{
  CostOptimizedMergedHCubeJoin,
  InMemoryScan,
  InMemoryScanExec,
  MergedHCubeLeapJoinExec
}
import org.apache.spark.adj.utils.misc.{Conf, SparkSingle}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

//TODO: debug it
class Sampler(relations: Seq[Relation],
              sampleInfos: SampleTaskInfo,
              databaseScaleRatio: Double = 0.25)
    extends Serializable {

  val sampledRelationMap = prepareSampledDataBase()
  var schemaToRelationMap =
    relations.map(relation => (relation.schema, relation)).toMap

  //For each input relation, construct a sampled relation
  private def prepareSampledDataBase(): Map[RelationSchema, Relation] = {
    val rdds = relations.map(_.rdd)
    val schemas = relations.map(_.schema)
    val ratioOfScaledDatabase = databaseScaleRatio
    val newSchemas = schemas.map { schema =>
      RelationSchema.tempSchemaWithAttrIds(schema.attrIDs)
    }

    val sampledRDDs = rdds.map { rdd =>
      val sampledRDD = rdd.mapPartitions { it =>
        Random.setSeed(System.nanoTime())
        it.filter { p =>
          Random.nextDouble() < ratioOfScaledDatabase
        }
      }

      val output = sampledRDD.cache()
      sampledRDD.count()

      output
    }

    val sampledRelations = newSchemas.zip(sampledRDDs).map {
      case (schema, rdd) =>
        schema.setContent(rdd)
        Relation(schema, rdd)
    }

    //add the statistic for the sampled relation
    val statistic = Statistic.defaultStatistic()
    sampledRelations.foreach(statistic.add)

    schemas.zip(sampledRelations).toMap
  }

  //reduce the testQuery according to sampled Relation
  private def prepareReducedDatabase(
    sampleRelation: Relation,
    sampleParameterTaskInfo: SampleParameterTaskInfo
  ): Seq[Relation] = {
    val testSchemas = sampleParameterTaskInfo.testQuery
    val testQuery = testSchemas.map(schemaToRelationMap)
    val sampleRDD = sampleRelation.rdd
    val sampleSchema = sampleRelation.schema

    //get the relations that share the attribute with the sampleSchema
    val relatedRelations = testQuery.filter { relation =>
      val attrIds = relation.schema.attrIDs
      attrIds.intersect(sampleSchema.attrIDs).nonEmpty
    }

    val samples = sampleRDD.collect()
    val filterSetPerAttr = sampleSchema.attrIDs.zipWithIndex.map {
      case (_, idx) =>
        val mutableSet = mutable.HashSet[Int]()
        samples.foreach { tuple =>
          mutableSet.add(tuple(idx))
        }
        (idx, mutableSet)
    }.toMap

    val reducedRelatedRelations = relatedRelations.map { relation =>
      val attrIds = relation.schema.attrIDs
      val intersectedAttrIds = attrIds.intersect(sampleSchema.attrIDs)
      val intersectedLocalPosToFilterSetPos = intersectedAttrIds
        .map(attrIds.indexOf)
        .zip(intersectedAttrIds.map(sampleSchema.attrIDs.indexOf))
        .toArray

      println(
        s"schema:${relation.schema}, sampleSchema:${sampleSchema}, intersectedLocalPosToFilterSetPos:${intersectedLocalPosToFilterSetPos.toSeq}"
      )

      val reducedRdd = relation.rdd.mapPartitions { it =>
        it.filter { tuple =>
          intersectedLocalPosToFilterSetPos.forall {
            case (localPos, filterPos) =>
              filterSetPerAttr(filterPos).contains(tuple(localPos))
          }
        }
      }

      val reducedSchema =
        RelationSchema.tempSchemaWithAttrIds(attrIds, reducedRdd)

      Relation(reducedSchema, reducedRdd)
    }

    val statistic = Statistic.defaultStatistic()
    (reducedRelatedRelations :+ sampleRelation).foreach(
      statistic.addCardinalityOnly
    )

    testQuery.diff(relatedRelations) ++ reducedRelatedRelations :+ sampleRelation

  }

  //generate the samples according to sampleQuery
  private def genSamples(
    sampleParameterTaskInfo: SampleParameterTaskInfo
  ): Relation = {
    val sampleQuery =
      sampleParameterTaskInfo.sampleQuery.map(sampledRelationMap)
    val sampleAttr = sampleParameterTaskInfo.sampleQueryAttrOrder
    val numSamples = sampleParameterTaskInfo.totalSamples / Conf
      .defaultConf()
      .numMachine
    val attrIdToPreserve = sampleParameterTaskInfo.sampledRelationSchema.attrIDs
      .map(sampleAttr.indexOf)
      .toArray

    //compute shares
    val shareComputer = new EnumShareComputer(
      sampleQuery.map(_.schema),
      Conf.defaultConf().numMachine
    )
    val share = shareComputer.optimalShare()._1

    val phyiscalPlan = MergedHCubeLeapJoinExec(
      null,
      sampleQuery.map(
        relation => InMemoryScanExec(relation.schema, relation.rdd)
      ),
      share,
      sampleAttr,
      share.values.product
    )

    val subTaskRDD = phyiscalPlan.subTaskRDD

    val sampledRelationRDD = subTaskRDD
      .flatMap { task =>
        val subJoinTask =
          SubTaskFactory.genSubTask(task.shareVector, task.blocks, task.info)

        val iterator =
          subJoinTask.execute()

        var i = 0
        val rawSamplesCount = numSamples * 10
        val secondSampleRatio = numSamples.toDouble / rawSamplesCount

        var rawSamples = ArrayBuffer[Array[Int]]()

        //get raw sampleRDD
        while (i < rawSamplesCount && iterator.hasNext) {
          val tuple = iterator.next()
          rawSamples += attrIdToPreserve.map(tuple)
          i += 1
        }

        //secondary sample
        Random.setSeed(System.currentTimeMillis())
        rawSamples = rawSamples.filter { sample =>
          Random.nextDouble() < secondSampleRatio
        }

        rawSamples.toArray.toIterator
      }

    val sampledRelationSchema = sampleParameterTaskInfo.sampledRelationSchema
    sampledRelationSchema.setContent(sampledRelationRDD)

    val sampledRelation = Relation(sampledRelationSchema, sampledRelationRDD)

    schemaToRelationMap = schemaToRelationMap ++ Seq(
      (sampledRelationSchema, sampledRelation)
    )

    sampledRelation
  }

  //perform the test Query
  private def performTest(sampleParameterTaskInfo: SampleParameterTaskInfo,
                          testQuery: Seq[Relation],
                          testRelationSize: Int) = {

    val testQueryAttrOrder = sampleParameterTaskInfo.testQueryAttrOrder

    //compute shares
    val shareComputer = new EnumShareComputer(
      testQuery.map(_.schema),
      Conf.defaultConf().numMachine
    )
    val share = shareComputer.optimalShare()._1

    val phyiscalPlan = MergedHCubeLeapJoinExec(
      null,
      testQuery.map(
        relation => InMemoryScanExec(relation.schema, relation.rdd)
      ),
      share,
      testQueryAttrOrder,
      share.values.product
    )

    val subTaskRDD = phyiscalPlan.subTaskRDD

    val parameters = subTaskRDD
      .map { task =>
        val subJoinTask =
          SubTaskFactory.genSubTask(task.shareVector, task.blocks, task.info)

        val iterator =
          subJoinTask.execute()

        iterator.recordCardinalityAndTime()
      }
      .collect()

    val timeParameter = parameters.map(_._2).sum
    val cardinalityParameter = parameters.map(_._1).sum

    (cardinalityParameter / testRelationSize, timeParameter / testRelationSize)

  }

  //perform the sampleTask
  private def performSampleTaskForOneParameter(
    sampleParameterTaskInfo: SampleParameterTaskInfo
  ): SampledParameter = {

    println(
      s"performSampleTaskForOneParameter for sampleParameterTaskInfo:${sampleParameterTaskInfo}"
    )

    val sampleRelation = genSamples(sampleParameterTaskInfo)
    val sampleSize = sampleRelation.rdd.count().toInt

    println(s"sampleSize:${sampleSize}")
    val testQuery =
      prepareReducedDatabase(sampleRelation, sampleParameterTaskInfo)
    val (avgCardinality, avgTime) =
      performTest(sampleParameterTaskInfo, testQuery, sampleSize)

    SampledParameter(
      sampleParameterTaskInfo.prevHyperNodes,
      sampleParameterTaskInfo.curHyperNodes,
      sampleParameterTaskInfo.ghd,
      sampleSize,
      avgCardinality,
      avgTime
    )
  }

  //Generate a relation that only contains a single attr from the database
  private def genAttrRelation(attrID: AttributeID): Relation = {
    val relatedRelations = relations.filter { relation =>
      relation.schema.attrIDs.contains(attrID)
    }

    val projectedRelatedRDDs = relatedRelations.map { relation =>
      val attrPos = relation.schema.attrIDs.indexOf(attrID)
      val projectedRDD = relation.rdd
        .mapPartitions { it =>
          it.map(f => f(attrPos))
        }
      projectedRDD
    }

    val numRelatedRDDs = projectedRelatedRDDs.size

//    println(s"numRelatedRDDs:${numRelatedRDDs}")

    if (numRelatedRDDs == 1) {
      val contentRDD = projectedRelatedRDDs.head.distinct().map(f => Array(f))
      val schema = RelationSchema.tempSchemaWithAttrIds(Seq(attrID), contentRDD)
      Relation(schema, contentRDD)
    } else if (numRelatedRDDs > 1) {

      val sc = SparkSingle.getSparkContext()
      val rawContentRDD = sc.union(projectedRelatedRDDs).distinct()

//      var i = 1
//      var rawContentRDD = projectedRelatedRDDs.head
//      while (i < numRelatedRDDs) {
//        val tempRDD = projectedRelatedRDDs(i)
//        rawContentRDD = rawContentRDD.intersection(tempRDD)
//        i += 1
//      }

      val contentRDD = rawContentRDD.map(f => Array(f))
      val schema = RelationSchema.tempSchemaWithAttrIds(Seq(attrID), contentRDD)
      Relation(schema, contentRDD)
    } else {
      throw new Exception(s"Cannot gen AttrRelation for attrID:${attrID}")
    }
  }

  //Sample an relation
  private def performSampleOnRelation(
    sampleParameterTaskInfo: SampleParameterTaskInfo,
    relation: Relation,
    numSample: Int
  ): Relation = {
    val contentRDD = relation.rdd
    val attrIds = relation.schema.attrIDs
    val ratio = numSample.toDouble / contentRDD.count()
    val sampledContentRDD = contentRDD.mapPartitions { it =>
      Random.setSeed(System.currentTimeMillis())
      it.filter { p =>
        Random.nextDouble() < ratio
      }
    }

    val sampledSchema =
      RelationSchema.tempSchemaWithAttrIds(attrIds, sampledContentRDD)

    sampleParameterTaskInfo.sampledRelationSchema = sampledSchema

    Relation(sampledSchema, sampledContentRDD)
  }

  //Perform sample task for the case where the prevHyperNodes is empty
  private def performSampleTaskForEmptyPrevNodeParameter(
    sampleParameterTaskInfo: SampleParameterTaskInfo
  ): SampledParameter = {

    println(
      s"performSampleTaskForEmptyPrevNodeParameter for sampleParameterTaskInfo:${sampleParameterTaskInfo}"
    )

    val testSchema = sampleParameterTaskInfo.testQuery
    val testQueryAttrOrder = sampleParameterTaskInfo.testQueryAttrOrder

    val firstAttr = testQueryAttrOrder
    val firstAttrRelation = genAttrRelation(firstAttr(0))
    val firstAttrSize = firstAttrRelation.rdd.count()
    val sampledFirstAttrRelation = performSampleOnRelation(
      sampleParameterTaskInfo,
      firstAttrRelation,
      sampleParameterTaskInfo.totalSamples
    )
    val sampleSize = sampledFirstAttrRelation.rdd.count().toInt

    println(s"firstAttrSize:${firstAttrSize}, sampleSize:${sampleSize}")

    val testQuery =
      prepareReducedDatabase(sampledFirstAttrRelation, sampleParameterTaskInfo)
    val (avgCardinality, avgTime) =
      performTest(sampleParameterTaskInfo, testQuery, sampleSize)

    SampledParameter(
      sampleParameterTaskInfo.prevHyperNodes,
      sampleParameterTaskInfo.curHyperNodes,
      sampleParameterTaskInfo.ghd,
      sampleSize,
      avgCardinality * firstAttrSize,
      avgTime * firstAttrSize
    )
  }

  //perform the sampling and generate the Seq[SampledParameter].
  def genSampledParameters(): Seq[SampledParameter] = {

    //TODO: delete this part after debug
//    val topKSample = 2

    sampleInfos.parameterTaskInfos
//      .slice(0, topKSample)
      .map(
        sampleInfo =>
          if (sampleInfo.prevHyperNodes.isEmpty) {
            performSampleTaskForEmptyPrevNodeParameter(sampleInfo)
          } else {
            performSampleTaskForOneParameter(sampleInfo)
        }
      )

  }
}
//    val infoOfSamples = sampleInfo
//    val hcubePlan = HCubePlan(sampledRelations, share)
//    val hcube = new PullHCube(hcubePlan, infoOfSamples)
//    val subTaskRDD = hcube.genHCubeRDD()
//
//    subTaskRDD
//      .map { task =>
//        val subSampleTask =
//          new SampleTask(
//            task.shareVector,
//            task.blocks,
//            task.info.asInstanceOf[SampleTaskInfo]
//          )
//
//        subSampleTask.genSampledParameters()
//      }
//      .collect()

//  private def scaleTheSampleParameters(
//    sampledParameters: Seq[Seq[SampledParameter]]
//  ): Seq[SampledParameter] = {
//    val summedParameters = sampledParameters
//      .flatMap(f => f)
//      .map { f =>
//        ((f.prevHyperNodes, f.curHyperNodes), f)
//      }
//      .groupBy(_._1)
//      .map {
//        case (key, parameters) =>
//          val headParameter = parameters.head._2
//          val summedValue = parameters.map { parameter =>
//            parameter._2.cardinalityValue
//          }.sum
//
//          headParameter.cardinalityValue = summedValue
//          headParameter
//      }
//      .toSeq
//
//    val scaledParameters = summedParameters.map { parameter =>
//      val ghd = parameter.ghd
//      val nodeIdToRelations = ghd.V.toMap
//      val coreAttrIds = parameter.prevHyperNodes
//        .map(nodeIdToRelations)
//        .flatMap(f => f)
//        .flatMap(_.attrIDs)
//        .toSeq
//      val leafAttrIds =
//        nodeIdToRelations(parameter.curHyperNodes).flatMap(_.attrIDs).distinct
//      val diffAttrIds = leafAttrIds.diff(coreAttrIds)
//      val shareForDiffAttrIds = diffAttrIds.map(share)
//      val scaleFactor = shareForDiffAttrIds.product
//      parameter.cardinalityValue = parameter.cardinalityValue * scaleFactor
//      parameter
//    }
//
//    scaledParameters
//  }

//  private val sampledRelations: Seq[Relation] = prepareSampledDataBase()
//  private var share: Map[AttributeID, Int] = prepareShare()

//  def prepareShare(): Map[AttributeID, Int] = {
//
//    val schemas = sampledRelations.map(_.schema)
//    val statistic = Statistic.defaultStatistic()
//    sampledRelations.foreach(statistic.add)
//
//    val shareComputer =
//      new EnumShareComputer(schemas, Conf.defaultConf().taskNum)
//    share = shareComputer.optimalShare()._1
//
//    share
//  }
