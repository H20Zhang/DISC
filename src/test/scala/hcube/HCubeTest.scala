package hcube

import org.apache.spark.adj.hcube.{HCubeHelper, RelationPartitioner}
import org.apache.spark.adj.utlis.SparkSingle
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import utils.TestingData

class HCubeTest extends FunSuite with BeforeAndAfterAll{

  val hcubePlan = TestingData.testing_query1_hcubePlan
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

  test("HCubeRDD"){

  }

  test("HCube"){

  }

  override protected def afterAll(): Unit = {
    SparkSingle.close()
  }

}
