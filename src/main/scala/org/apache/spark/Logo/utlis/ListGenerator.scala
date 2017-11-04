package org.apache.spark.Logo.utlis

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object ListGenerator {

  def fillList[T:ClassTag](ele:T, size:Int) = {
    val list = new Array[T](size).toList
    list.map(f => ele)
  }

  def crossProduct[T:ClassTag](lList:List[List[T]],rList:List[T]): List[List[T]] ={
    lList.flatMap(f =>
      rList.map(x => f :+ x)
    )
  }



  def cartersianList(size:Int):List[Int] = {
    List.range(0,size)
  }

  def cartersianList(sizeList:List[Int]):List[List[Int]] = {
    sizeList.size match {
      case 0 => {
        throw new Exception("sizeList must not be empty")
      }
      case 1 => {
        List.range(0,sizeList(0)).map(f => List(f))
      }
      case _ => {
        val newSizeList = sizeList.drop(1)
        val startList = cartersianList(List(sizeList(0)))
        newSizeList.foldLeft(startList)((x,y) => crossProduct[Int](x,cartersianList(y)))
      }
    }
  }

  def fillListIntoSlots(list:List[Int], totalSlot:Int, slotMapping:List[Int]):List[Int] = {
    val resultList = fillList(0,totalSlot).toArray
    slotMapping.zipWithIndex.foreach{case (z,index) => resultList(z) = list(index)}
    resultList.toList
  }

//  def fillListListIntoSlots(catersianList:List[List[Int]], totalSlot:Int, slotMapping:List[Int]): List[List[Int]] ={
//    val listSize = catersianList.length
//    val resultList = fillList(0,listSize).map(f => fillList(0,totalSlot))
//    catersianList.zip(resultList).map{case (x,y) =>
//      val array = Array.concat[Int](y.toArray)
//        slotMapping.zipWithIndex.foreach{case (z,index) => array(z) = x(index)}
//        array.toList
//    }
//  }


  def fillListListIntoSlots(catersianList:List[List[Int]], totalSlot:Int, slotMapping:List[Int]): List[List[Int]] ={
    catersianList.map(list => fillListIntoSlots(list,totalSlot,slotMapping))
  }





}
