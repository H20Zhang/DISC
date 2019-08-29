package deprecated.Underlying

import org.apache.spark.adj.deprecated.utlis._
import org.scalatest.FunSuite

class UtilsTest extends FunSuite{

  test("listGenerator"){
    //test list cross product
    val list1 = List.range(0,3).map(f => List(f))
    val list2 = List.range(1,4)
    val list3 = ListGenerator.crossProduct(list1, list2)

    val list4 = List(
      List(0,1),List(0,2),List(0,3),
      List(1,1),List(1,2),List(1,3),
      List(2,1),List(2,2),List(2,3)
      )

    list4.foreach(println)
    list3.foreach(println)

    assert(list3.zip(list4).forall(p => (p._1.diff(p._2).length == 0)))

    //test fill list
    val emptyList = ListGenerator.fillList(0,10)
    assert(emptyList.size == 10)


    // test generate catersian list
    val sizeList = List(3,3)
    val cartersianList = ListGenerator.cartersianSizeList(sizeList)
    val cartersianList1 = List(
      List(0,0),List(0,1),List(0,2),
      List(1,0),List(1,1),List(1,2),
      List(2,0),List(2,1),List(2,2)
    )

    assert(cartersianList.zip(cartersianList1).forall(p => (p._1.diff(p._2).length == 0)))

    //test fill cartersian list
    val slotMapping = List(1,2)
    val resultList = ListGenerator.fillListListIntoSlots(cartersianList,3,slotMapping)

    val resultList1 = List(
      List(0,0,0),List(0,0,1),List(0,0,2),
      List(0,1,0),List(0,1,1),List(0,1,2),
      List(0,2,0),List(0,2,1),List(0,2,2)
    )

    resultList.foreach(println)


    assert(resultList.zip(resultList1).forall(p => (p._1.diff(p._2).length == 0)))


    System.err.print("Error Test")
  }


  test("PointToNumConverter"){
    val baseList = List(3,4,5,6)
    val numList = List(1,2,3,4)
    val converter = new PointToNumConverter(baseList)
    val index = converter.convertToNum(numList)

    assert(index == 1+2*3+3*3*4+4*3*4*5)

    val numList1 = converter.NumToList(index)

    assert(numList.diff(numList).length == 0)
  }

  test("ListSelector"){
    val list = List(1,2,3,4,5)
    val keys = List(0,2,4)
    val selectedList = ListSelector.selectElements(list,keys.toSet)
    val selectedList1 = List(1,3,5)
    assert(TestUtil.listEqual(selectedList1,selectedList))
  }

  test("mapBuilder"){
    val list1 = List(1,2,3,4)
    val list2 = List(2,3,4,5)
    val list3 = List(3,4,5,6)
    val list4 = List(4,5,6,7)
    val list5 = List(5,6,7,8)

    val keys = List(0,1)
    val values= List(2)
    val total = List(0,1,2,3)
    val list = List(list1,list2,list3,list4,list5)

    val theMap = MapBuilder.fromListToMap(list,keys.toSet)

    assert(TestUtil.listlistEqual(List(List(3,4)),theMap.get(List(1,2)).get))
  }

  test("KeyValueMap"){
    val list1 = List(1,2,3,4)
    val list2 = List(2,3,4,5)
    val list3 = List(3,4,5,6)
    val list4 = List(4,5,6,7)
    val list5 = List(5,6,7,8)

    val keys = List(0,1)
    val values= List(2)
    val total = List(0,1,2,3)
    val list = List(list1,list2,list3,list4,list5)

//    val theMap = MapBuilder.buildKeyValueMap(list,keys,values)
//    println(TestUtil.listlistEqual(List(List(4)),theMap.get(List(1,2)).get.get(List(3)).get))
  }

    test("converter"){
      val converter = new PointToNumConverter(Seq(3,3,1))



      var num = 1

      for(i <- 0 until 9) {
        num = i
        val list = converter.NumToList(num)
        println(converter.NumToList(num))

        println(converter.convertToNum(list))

        assert(converter.convertToNum(list) == i)
      }

    }

//  test("testNewHashMap"){
//    val list1 = List(1,2,3,4)
//    val list2 = List(2,3,4,5)
//    val list3 = List(3,4,5,6)
//    val list4 = List(4,5,6,7)
//    val list5 = List(5,6,7,8)
//
//    val keys = List(0,1)
//    val values= List(2)
//    val total = List(0,1,2,3)
//    val list = List(list1,list2,list3,list4,list5)
//
//    val theMap = MapBuilder.fromListToMapFast(list,keys.toSet)
//
//
//    assert(TestUtil.listlistEqual(List(List(3,4)),theMap.get(List(1,2))))
//  }


}
