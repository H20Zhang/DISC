package org.apache.spark.Logo.Physical.dataStructure

import org.apache.spark.Logo.Physical.Joiner.multiJoin.StarJoin
import org.apache.spark.Logo.Physical.utlis.TestUtil
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class dataStructureTest extends FunSuite {



//  implicit class Crossable[X](xs: Traversable[X]) {
//    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
//  }

//  test("3 star join test"){
//
//    val p1 = 7
//    val p2 = 100
//
//    val res = (Range(0,p1) cross Range(0,p1)).toSeq
//    val rdd1 = sc.parallelize(res).map(f => (f,f))
//    val rdd2 = sc.parallelize(Range(0,p2)).map(f => (f,f))
//    val rdd3 = sc.parallelize(Range(0,p2)).map(f => (f,f))
//
//    val resRDD = StarJoin.StarJoin3(sc,rdd1,rdd2,rdd3)({(a,b,c) =>
//      a.foreach(println)
//      b.foreach(println)
//      c.foreach(println)
//
//      a
//      })
//
//    val r = resRDD.map(f => f).count()
//    print(r)
//  }


  test("SlotPartitioner"){
    val slotPartitioner0 = new SlotPartitioner(20,0)
    val slotPartitioner1 = new SlotPartitioner(10,1)
    val slotPartitioner2 = new SlotPartitioner(25,2)

    val key1 = List(1,2)
    assert(slotPartitioner0.getPartition(key1) == 1)
    assert(slotPartitioner1.getPartition(key1) == 2)

    try{
      slotPartitioner2.getPartition(key1)
    } catch{
      case exception:Exception => assert(exception.getMessage == "slotNum must be smaller or equal to the total slots of the key")
    }


    val key2 = List(21,32,26)
    assert(slotPartitioner0.getPartition(key2) == 1)
    assert(slotPartitioner1.getPartition(key2) == 2)
    assert(slotPartitioner2.getPartition(key2) == 1)
  }

  test("CompositePartitioner"){

    val slotPartitioner0 = new SlotPartitioner(20,0)
    val slotPartitioner1 = new SlotPartitioner(10,1)
    val slotPartitioner2 = new SlotPartitioner(25,2)
    val compositeParitioner2D1 = new CompositeParitioner(List(slotPartitioner0,slotPartitioner1))
    val compositeParitioner2D2 = new CompositeParitioner(List(slotPartitioner1,slotPartitioner2))
    val compositeParitioner3D = new CompositeParitioner(List(slotPartitioner0,slotPartitioner1,slotPartitioner2))
//    val compositeParitioner2D2D = new CompositeParitioner(List(compositeParitioner2D1,compositeParitioner2D2))
//    val limitsCompositePartitioner2D2D = new CompositeParitioner(List(compositeParitioner2D1,compositeParitioner2D2), List(20,20))


    val key1 = List(24,35,27)
    assert(compositeParitioner2D1.getPartition(key1) == 4+5*20)
    assert(compositeParitioner2D2.getPartition(key1) == 5+2*10)
    assert(compositeParitioner3D.getPartition(key1) == 4+5*20+2*20*10)
//    assert(compositeParitioner2D2D.getPartition(key1) == 85*20*10+52)
//    assert(limitsCompositePartitioner2D2D.getPartition(key1) == 5*20+12)

    val key2 = (24,35,27)
    assert(compositeParitioner2D1.getPartition(key2) == 4+5*20)
    assert(compositeParitioner2D2.getPartition(key2) == 5+2*10)
    assert(compositeParitioner3D.getPartition(key2) == 4+5*20+2*20*10)
//    assert(compositeParitioner2D2D.getPartition(key2) == 85*20*10+52)

  }

  test("LogoSchema"){

    //logoSchema
    val edges = List((0,1),(1,2),(2,0))
    val keySizeMap = Map((0,3),(1,3),(2,3))
    val triangleSchema = LogoSchema(keySizeMap)

    //compositeLogoSchema
    val intersectionKeyMappings = List(
      Map((0,0),(1,1)),
      Map((0,1),(1,2),(2,0)),
      Map((0,2),(2,0))
    )

    val edges1 = List((0,1),(1,2),(0,2))
    val keySizeMap1 = Map((0,3),(1,3),(2,3))
    val triangleSchema1 = LogoSchema(keySizeMap1)

    //for three triangle pattern, as the edges are directed, the last triangle is actually different from the other two
    //in the sense of the edge direction.
    val oldSchemas = List(triangleSchema,triangleSchema,triangleSchema1)

    val threeTriangleSchema = CompositeLogoSchema(oldSchemas,intersectionKeyMappings.map(f => KeyMapping(f)))

    val edges2 = List((0,1),(1,3),(3,0),(1,2),(2,4),(4,0),(2,0)).map{
      f =>
        if (f._2 < f._1){
          f.swap
        }else{
          f
        }
    }
    val keySizeMap2 = Map((0,3),(1,3),(2,3),(3,3),(4,3))
    val keyMapping2 = List(
      List(0,1,3),
      List(1,2,0),
      List(2,4,0)
    )

//    threeTriangleSchema.keyMapping.foreach(println)


    assert(TestUtil.listEqual(keySizeMap2.toList.sorted,threeTriangleSchema.keySizeMap.toList.sorted), "CompositeLogoSchema's generate KeySizeMap wrong")
    assert(TestUtil.listlistEqual(keyMapping2,threeTriangleSchema.keyMappings.map(_.toListMapping())), "CompositeLogoSchema's generated keyMapping wrong")

    //compositeKey conversion Test


    val newKey = List(1,2,2,2,2)
    val newIndex = threeTriangleSchema.keyToIndex(newKey)
    val oldKeys = threeTriangleSchema.newKeyToOldKey(newKey)
    val oldIndex = threeTriangleSchema.newIndexToOldIndex(newIndex)


    val oldKeys1 = List(
      List(1,2,2),
      List(2,2,1),
      List(2,2,1))
    val oldIndex1 = List(25,17,17)

    assert(TestUtil.listlistEqual(oldKeys1,oldKeys))
    assert(TestUtil.listEqual(oldIndex1.sorted,oldIndex1.sorted))


    val newIndex1 = threeTriangleSchema.oldIndexToNewIndex(oldIndex1)
    assert(newIndex==newIndex1)

    println(newIndex1)



  }

}
