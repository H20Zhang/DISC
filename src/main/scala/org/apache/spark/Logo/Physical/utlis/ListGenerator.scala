package org.apache.spark.Logo.Physical.utlis

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object ListGenerator {


  /**
    *
    * @param ele element used to fill the list
    * @param size list length
    * @tparam T list element type
    * @return a list of length "size" filled with element "ele"
    */
  def fillList[T:ClassTag](ele:T, size:Int) = {
    val list = new Array[T](size).toList
    list.map(f => ele)
  }


  /**
    * @example
    *          given lList: List(List(1),List(2))
    *                rList: is List(2,3)
    *                then the result list will be List(List(1,2),List(1,3),List(2,2),List(2,3))
    * @param lList a list of list
    * @param rList list to multiple lList
    * @tparam T element of lList's sublist and rList
    * @return a cartersianed list
    */
  def crossProduct[T:ClassTag](lList:List[List[T]],rList:List[T]): List[List[T]] ={
    lList.flatMap(f =>
      rList.map(x => f :+ x)
    )
  }



  private def cartersianSizeList(size:Int):List[Int] = {
    List.range(0,size)
  }

  /** @example
    *          give sizeList: List(3,3,3)
    *          then the result list would be List(List(0,0,0),List(0,0,1),List(0,0,2),List(0,1,0),List(0,1,1)
    *          ,List(0,2,0),List(0,2,1),List(0,2,2),List(1,0,0),List(1,0,1),List(1,0,2),List(1,1,0),List(1,1,1)
    *          ,List(1,2,0),List(1,2,1),List(1,2,2),List(2,2,2))
    * @param sizeList sizeLimit of each slot
    * @return generate a catersian list that will have all combination of number that is below sizeLimit for each slot
    */
  def cartersianSizeList(sizeList:List[Int]):List[List[Int]] = {
    sizeList.size match {
      case 0 => {
        throw new Exception("sizeList must not be empty")
      }
      case 1 => {
        List.range(0,sizeList(0)).map(f => List(f))
      }
      case _ => {
        val newSizeList = sizeList.drop(1)
        val startList = cartersianSizeList(List(sizeList(0)))
        newSizeList.foldLeft(startList)((x,y) => crossProduct[Int](x,cartersianSizeList(y)))
      }
    }
  }

  def constrainedCartersianSizeList(sizeList:List[Int], slotMapping:List[Int], targetList:List[Int]) = {
    fillCartersianListIntoTargetList(cartersianSizeList(sizeList),targetList.length, slotMapping,targetList)
  }


  def fillListIntoTargetList(list:List[Int], totalSlot:Int, slotMapping:List[Int], targetList:List[Int]) = {
    val resultList = targetList.toArray
    slotMapping.zipWithIndex.foreach{case (z,index) => resultList(z) = list(index)}
    resultList.toList
  }

  def fillCartersianListIntoTargetList(catersianList:List[List[Int]], totalSlot:Int, slotMapping:List[Int], targetList:List[Int]) ={
    catersianList.map(list => fillListIntoTargetList(list,totalSlot,slotMapping,targetList))
  }

  /**
    * @example list: List(1,2),
    *          totalSlot: 3
    *          slotMapping: List(0,1)
    *          result list would be List(1,2,0),
    *          0 is filled at the non-filled position
    * @param list a list
    * @param totalSlot total slot
    * @param slotMapping a map between "list" position to slot position
    * @return
    */
  def fillListIntoSlots(list:List[Int], totalSlot:Int, slotMapping:List[Int]):List[Int] = {
    fillListIntoTargetList(list,totalSlot,slotMapping,fillList(0,totalSlot))
  }

  def fillListListIntoSlots(catersianList:List[List[Int]], totalSlot:Int, slotMapping:List[Int]): List[List[Int]] ={
    fillCartersianListIntoTargetList(catersianList,totalSlot,slotMapping,fillList(0,totalSlot))
  }

}
