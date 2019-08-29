package hcube

import org.apache.spark.adj.hcube.{HCube, HCubeHelper, RelationPartitioner, TupleHCubeBlock}
import org.apache.spark.adj.plan.AttributeOrderInfo
import org.apache.spark.adj.utils.SparkSingle
import org.apache.spark.adj.utils.testing.TestingSubJoins
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

class HCubeTest extends FunSuite with BeforeAndAfterAll{

  val hcubePlan = TestingSubJoins.testing_query1_hcubePlan
  val helper = new HCubeHelper(hcubePlan)
  val testingRelationID = 1

  test("HCubeHelper -- generate partitioner"){

    //testing partitioner for R1
    val partitionerForR1 = helper.partitionerForRelation(testingRelationID)
    val contentForR1 = hcubePlan.idForRelation(testingRelationID).content.collect()

    val isPartitionerValid = contentForR1.forall{
      tuple =>
        val tupleHashValue = partitionerForR1.getPartition(tuple)
        tupleHashValue == partitionerForR1.getPartition(partitionerForR1.getShare(tupleHashValue))
    }

    assert(isPartitionerValid, "partitioner for R1 didn't work correctly")
  }

  test("HCubeHelper -- generate sentry"){
    val sentry = helper.genShareForRelation(testingRelationID)
    println(sentry.map(_.toSeq).toSeq)
  }

  test("RelationPartitioner"){
    val relationPartitioner = new RelationPartitioner(hcubePlan.idForRelation(testingRelationID), helper)
    val partitionedRelation = relationPartitioner.partitionRelation()
    val numOfBlocks = partitionedRelation.mapBlock{
      f =>
        Iterator(1)
    }.count()

    val partitionerForR1 = helper.partitionerForRelation(testingRelationID)
    val isPartitionCorrectly = partitionedRelation.partitionedRDD.mapPartitionsWithIndex{
      case (idx, f) =>
        Iterator((f.next().shareVector, idx))
    }.collect().forall{
      case (shareVector, idx) =>
        idx == partitionerForR1.getPartition(shareVector)
    }

    assert(isPartitionCorrectly, "not correctly partitioned")

    println(s"number of blocks: ${numOfBlocks}")
  }

  test("HCube -- generate hcubeRDD"){

    val taskInfo = AttributeOrderInfo(Array(0, 1, 2, 3, 4))
    val hcube = new HCube(hcubePlan, taskInfo)

    hcube.genHCubeRDD().flatMap{
      subtask =>
        val subjoin = subtask.toSubJoin()
        val blocks = subjoin.blocks
        val shareVectors = blocks.map(_.shareVector)
        val relationSchemas = blocks.map(_.schema)

        println(s"subjoin share:${subjoin.shareVector.toSeq}, shares:${shareVectors.map(_.toSeq)}, attrsForeachRelation:${relationSchemas.map(_.attrIDs)}")

        blocks.combinations(2).map{
          blockPair =>
            val lBlock = blockPair(0)
            val rBlock = blockPair(1)
            val lSchema = lBlock.schema
            val rSchema = rBlock.schema
            val commonAttributes = lSchema.attrIDs.intersect(rSchema.attrIDs)

            if (!commonAttributes.isEmpty){
              val isAccordingShareSame = commonAttributes.forall{
                attrID =>
                  val lPos = lSchema.attrIDs.indexOf(attrID)
                  val rPos = rSchema.attrIDs.indexOf(attrID)
                  lBlock.shareVector(lPos) == rBlock.shareVector(rPos)
              }
              isAccordingShareSame
            } else {
              true
            }
        }
    }.collect().foreach(p => assert(p, s"some thing wrong in distribute blocks in HCubeRDD"))

//    println(s"number of subtasks:${hcube.genHCubeRDD().count()}")

  }

  override protected def afterAll(): Unit = {
    SparkSingle.close()
  }

}
