import org.apache.spark.Logo.Maker.{PartitionerMaker, rowBlockGenerator}
import org.apache.spark.Logo.dataStructure.LogoSchema
import org.scalatest.FunSuite

class BlockMakerTest extends FunSuite{

  test("LogoBlockGenerator"){


    val edges = List((0,1),(0,2),(1,2))
    val keySizeMap = Map((0,3),(1,3))
    val schema = LogoSchema(edges, keySizeMap)


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

  }

  test("PartitionerMaker"){
    val compositeParitioner = PartitionerMaker().setSlotMapping(List(0,1)).setSlotSize(List(3,3)).build()
    assert(compositeParitioner.numPartitions == 9)
  }
}
