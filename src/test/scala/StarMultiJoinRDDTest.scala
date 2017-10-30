package org.apache.spark.Logo.dataStructure

import org.apache.spark.Logo.multiJoin.StarJoin
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class dataStructureTest extends FunSuite {

  val spark = SparkSession.builder().master("local[*]").appName("spark sql example").config("spark.some.config.option", "some-value")
    .getOrCreate()
  val sc = spark.sparkContext

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

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

  }


}
