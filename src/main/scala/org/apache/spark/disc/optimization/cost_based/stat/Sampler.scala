package org.apache.spark.disc.optimization.cost_based.stat

import org.apache.spark.disc.catlog.Catalog.{AttributeID, DataType}
import org.apache.spark.disc.catlog.{Relation, Schema}
import org.apache.spark.disc.execution.subtask.SubTaskFactory
import org.apache.spark.disc.optimization.cost_based.comp.EnumShareComputer
import org.apache.spark.disc.plan.InMemoryScanExec
import org.apache.spark.disc.plan
import org.apache.spark.disc.plan.{InMemoryScanExec, MergedHCubeLeapJoinExec}
import org.apache.spark.disc.util.misc.{Conf, SparkSingle}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class Sampler(relations: Seq[Relation],
              sampleInfos: SampleTaskInfo,
              databaseScaleRatio: Double = 0.25)
    extends Serializable {

  val sampledRelationMap = prepareSampledDataBase()
  var schemaToRelationMap =
    relations.map(relation => (relation.schema, relation)).toMap

  //For each input relation, construct a sampled relation
  private def prepareSampledDataBase(): Map[Schema, Relation] = {
    val rdds = relations.map(_.rdd)
    val schemas = relations.map(_.schema)
    val ratioOfScaledDatabase = databaseScaleRatio
    val newSchemas = schemas.map { schema =>
      Schema.tempSchemaWithAttrIds(schema.attrIDs)
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
        val mutableSet = mutable.HashSet[DataType]()
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

//      println(
//        s"schema:${relation.schema}, sampleSchema:${sampleSchema}, intersectedLocalPosToFilterSetPos:${intersectedLocalPosToFilterSetPos.toSeq}"
//      )

      val reducedRdd = relation.rdd.mapPartitions { it =>
        it.filter { tuple =>
          intersectedLocalPosToFilterSetPos.forall {
            case (localPos, filterPos) =>
              filterSetPerAttr(filterPos).contains(tuple(localPos))
          }
        }
      }

      val reducedSchema =
        Schema.tempSchemaWithAttrIds(attrIds, reducedRdd)

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
      .NUM_CORE
    val attrIdToPreserve = sampleParameterTaskInfo.sampledRelationSchema.attrIDs
      .map(sampleAttr.indexOf)
      .toArray

    //compute shares
    val shareComputer = new EnumShareComputer(
      sampleQuery.map(_.schema),
      Conf.defaultConf().NUM_CORE
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

        //can be set larger to improve the accuracy on the skewed dataset
        var rawSamplesCount = numSamples * 10

        var rawSamples = ArrayBuffer[Array[DataType]]()

        //get raw sampleRDD
        while (i < rawSamplesCount && iterator.hasNext) {
          val tuple = iterator.next()
          rawSamples += attrIdToPreserve.map(tuple)
          i += 1
        }

        val secondSampleRatio = numSamples.toDouble / rawSamples.size

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
      Conf.defaultConf().NUM_CORE
    )
    val share = shareComputer.optimalShare()._1

    val phyiscalPlan = plan.MergedHCubeLeapJoinExec(
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

        val numTest = 2
        val testResults = Range(0, numTest)
          .map { idx =>
            val iterator =
              subJoinTask.execute()
            iterator.recordCardinalityAndTime()
          }
          .drop(1)

        (
          testResults.map(_._1).sum / (numTest - 1).toDouble,
          testResults.map(_._2).sum / (numTest - 1).toDouble
        )
//
//        val iterator =
//          subJoinTask.execute()
//
//        iterator.recordCardinalityAndTime()
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

//    println(
//      s"performSampleTaskForOneParameter for sampleParameterTaskInfo:${sampleParameterTaskInfo}"
//    )

    val sampleRelation = genSamples(sampleParameterTaskInfo)
    val sampleSize = sampleRelation.rdd.count().toInt

//    println(s"sampleSize:${sampleSize}")
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

  //Generate a relation that only contains a single attr from the adj.database
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

    if (numRelatedRDDs == 1) {
      val contentRDD = projectedRelatedRDDs.head.distinct().map(f => Array(f))
      val schema = Schema.tempSchemaWithAttrIds(Seq(attrID), contentRDD)
      Relation(schema, contentRDD)
    } else if (numRelatedRDDs > 1) {

      val sc = SparkSingle.getSparkContext()
      val rawContentRDD = sc.union(projectedRelatedRDDs).distinct()

      val contentRDD = rawContentRDD.map(f => Array(f))
      val schema = Schema.tempSchemaWithAttrIds(Seq(attrID), contentRDD)
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
      Schema.tempSchemaWithAttrIds(attrIds, sampledContentRDD)

    sampleParameterTaskInfo.sampledRelationSchema = sampledSchema

    Relation(sampledSchema, sampledContentRDD)
  }

  //Perform sample task for the case where the prevHyperNodes is empty
  private def performSampleTaskForEmptyPrevNodeParameter(
    sampleParameterTaskInfo: SampleParameterTaskInfo
  ): SampledParameter = {

//    println(
//      s"performSampleTaskForEmptyPrevNodeParameter for sampleParameterTaskInfo:${sampleParameterTaskInfo}"
//    )

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

//    println(s"firstAttrSize:${firstAttrSize}, sampleSize:${sampleSize}")

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

    sampleInfos.parameterTaskInfos
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
