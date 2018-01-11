package Underlying

import org.apache.spark.Logo.UnderLying.Maker.{PartitionerMaker, SimpleRowLogoRDDMaker, rowBlockGenerator}
import org.apache.spark.Logo.UnderLying.dataStructure.{KeyMapping, LogoSchema}
import org.apache.spark.Logo.UnderLying.utlis.{SparkSingle, TestUtil}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class BlockMakerTest extends FunSuite with BeforeAndAfterAll{

  val (spark,sc) = SparkSingle.getSpark()

  test("LogoBlockGenerator"){

    val edges = List((0,1),(0,2),(1,2))
    val keySizeMap = Map((0,3),(1,3),(2,3))
    val schema = LogoSchema(keySizeMap)

    val index = schema.partitioner.getPartition(List(0,1,2))

    val data = List(
      (List(0,1,2),1),
      (List(0,1,2),1),
      (List(0,1,2),1),
      (List(0,1,2),1)
    ).toIterator

    val blockGenerator = new rowBlockGenerator(schema, index, data)

  }

  test("LogoRDDMaker"){

    val data = List.range(0,10).map(f => (f,f)).map(f => (Seq(f._1,f._2),1))


    val rawRDD = sc.parallelize(data)

    val edges = List((0,1))
    val keySizeMap = Map((0,3),(1,3))

    val logoRDDMaker = new SimpleRowLogoRDDMaker(rawRDD,1).setEdges(edges).setKeySizeMap(keySizeMap)

    val logoRDD = logoRDDMaker.build()

    //length match
    val collectedBlock = logoRDD.collect()
    val lengthList = List(4,0,0,0,3,0,0,0,3)
    assert(TestUtil.listEqual(collectedBlock.map(f => f.metaData.numberOfParts),lengthList),"each block's size must match")

    //index match
    val indexList = logoRDD.mapPartitionsWithIndex{case (index,list) =>
      Iterator(index)
    }.collect()
    val indexList1 = List(0,1,2,3,4,5,6,7,8)
    assert(TestUtil.listEqual(indexList,indexList1),"each block's index must appear")
  }

  test("PartitionerMaker"){
    val compositeParitioner = PartitionerMaker().setSlotSizeMapping(KeyMapping(List(3,3))).build()
    assert(compositeParitioner.numPartitions == 9)
  }


  override def afterAll(): Unit = {
    SparkSingle.close()
  }

}
