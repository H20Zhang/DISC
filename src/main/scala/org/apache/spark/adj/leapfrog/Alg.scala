package org.apache.spark.adj.leapfrog

import org.apache.spark.adj.database.Database.DataType

import scala.collection.mutable.ArrayBuffer

object Alg {

  def binarySearch[T](array: Array[T], value: T)(implicit arithmetic: Numeric[T]): Int =
    BSearch.search(array, value)

  def binarySearch(array: Array[Int], value: Int, _left:Int, _right:Int)(implicit arithmetic: Numeric[Int]): Int =
    BSearch.search(array,value,_left, _right)

  def mergelikeIntersection(arrays:Array[ArraySegment]):ArraySegment = Intersection.mergeLikeIntersection(arrays)

  def mergelikeIntersection(arrays:Array[Array[Int]]):Array[Int] = {
    val segmentArrays = arrays.map(arr => ArraySegment(arr))
    Intersection.mergeLikeIntersection(segmentArrays).toArray()
  }

  def leapfrogIntersection(arrays:Array[ArraySegment]):ArraySegment = Intersection.leapfrogIntersection(arrays)

  def leapfrogIntersection(arrays:Array[Array[Int]]):Array[Int] = {
    val segmentArrays = arrays.map(arr => ArraySegment(arr))
    Intersection.leapfrogIntersection(segmentArrays).toArray()
  }
}

object BSearch {
  def search[T](array: Array[T], value: T)(implicit arithmetic: Numeric[T]):Int = {
    var left: Int = 0;
    var right: Int = array.length - 1;
    while (right > left) {
      val mid = left + (right - left) / 2
      val comp = arithmetic.compare(array(mid), value)
      if (comp == 0)
        return mid; //negative if test < value
      else if (comp > 0) //list(mid) > value
        right = mid;
      else if (comp < 0) //list(mid) < value
        left = mid + 1;
    }
    -1;
  }

  def search(array: Array[Int], value: Int, _left:Int, _right:Int): Int = {
    var left: Int = _left;
    var right: Int = _right;
    while (right > left) {
      val mid = left + (right - left) / 2
      val midVal = array(mid)
//      val comp = arithmetic.compare(array(mid), value)

      var comp = 0
      if (midVal > value){
        comp = 1
      } else if(midVal < value){
        comp = -1
      }

      if (comp == 0)
        return mid; //negative if test < value
      else if (comp > 0) //list(mid) > value
        right = mid;
      else if (comp < 0) //list(mid) < value
        left = mid + 1;
    }
    -1;
  }
}


object IntersectionIterator{
  def leapfrogIt(arrays:Array[ArraySegment]):Iterator[DataType] =  {
    //    check some preliminary conditions
    if (arrays.size == 1){
      return arrays(0).toIterator
    }

    var i = 0
    while(i < arrays.size){
      if (arrays(i).size == 0){
        return Iterator.empty
      }
      i = i+1
    }

    new LeapFrogUnaryIterator(arrays)
  }

  def listIt(arrays:Array[ArraySegment]):Iterator[DataType] = new IntersectedListIterator(arrays)
}

class IntersectedListIterator(arrays:Array[ArraySegment]) extends Iterator[DataType]{

  val content = Alg.leapfrogIntersection(arrays)
  var idx = -1
  var end = content.size

  override def hasNext: Boolean = {
    (idx+1) < end
  }

  override def next(): DataType = {
    idx += 1
    content(idx)
  }
}

//TODO: debug
class LeapFrogUnaryIterator(arrays:Array[ArraySegment]) extends Iterator[DataType]{

  var value = Int.MaxValue
  val numOfArrays = arrays.size
  var maximalElement = Int.MinValue
  var isEnd = false
  var currentPosOfArrays = new Array[Int](numOfArrays)
  var count = 0
  var p = 0

  init()

  private def init() = {

    //    find maximum element at the first position
    var i = 0

    while (i < numOfArrays){
      val curVal = arrays(i)(0)
      if (curVal > maximalElement){
        maximalElement = curVal
        p = (i + 1) % numOfArrays
      }
      i = i + 1
    }
    count = 1

    //  init current position
    while(i < numOfArrays){
      currentPosOfArrays(i) = 0
      i = i + 1
    }
  }

  override def hasNext: Boolean = {

    //    intersect the arrays
    while(!isEnd){
      val curArray = arrays(p)
      val pos = Intersection.seek(curArray, maximalElement, currentPosOfArrays(p))
      var curPos = pos

      if (curPos < curArray.size) {
        val curVal = curArray(curPos)

        if (curVal == maximalElement) {

          count += 1
          if (count == numOfArrays) {
            count = 1
            value = maximalElement
            return isEnd
          }
        } else {
          count = 1
          maximalElement = curVal
        }
      } else {
        isEnd = true
      }

      currentPosOfArrays(p) = curPos
      p = (p + 1) % numOfArrays
    }


    !isEnd
  }

  override def next(): DataType = {
    value
  }
}

object Intersection {
  //  find the position i where array(i) >= value and i is the minimal value
  //  noted: the input array should be sorted
  def seek(array: ArraySegment, value: Int, _left:Int): Int = {
    var left: Int = _left;
    var right: Int = array.size;

    while (right > left) {
      val mid = left + (right - left) / 2
      val midVal = array(mid)

      var comp = 0
      if (midVal > value){
        comp = 1
      } else if(midVal < value){
        comp = -1
      }

      if (comp == 0)
        return mid; //negative if test < value
      else if (comp > 0) //list(mid) > value
        right = mid ;
      else if (comp < 0) //list(mid) < value
        left = mid + 1;
    }

    right
  }

  def leapfrogIntersection(arrays:Array[ArraySegment]):ArraySegment = {

//    check some preliminary conditions
    if (arrays.size == 1){
      return arrays(0)
    }

    val buffer = new ArrayBuffer[Int]()

    var i = 0
    while(i < arrays.size){
      if (arrays(i).size == 0){
        return ArraySegment(buffer.toArray)
      }
      i = i+1
    }

//    find maximum element at the first position
    val numOfArrays = arrays.size
    var maximalElement = Int.MinValue

    i = 0
    var p = 0
    while (i < numOfArrays){
      val curVal = arrays(i)(0)
      if (curVal > maximalElement){
        maximalElement = curVal
        p = (i + 1) % numOfArrays
      }
      i = i + 1
    }

//    intersect the arrays
    var isEnd = false
    var currentPosOfArrays = new Array[Int](numOfArrays)


    var count = 1

    i = 0
    while(i < numOfArrays){
      currentPosOfArrays(i) = 0
      i = i + 1
    }

    while(!isEnd){
      val curArray = arrays(p)
      val pos = seek(curArray, maximalElement, currentPosOfArrays(p))
      var curPos = pos

      if (curPos < curArray.size) {
        val curVal = curArray(curPos)

        if (curVal == maximalElement) {

          count += 1
          if (count == numOfArrays) {
            count = 1
            buffer += maximalElement

            curPos += 1
            if (curPos < curArray.size) {
              maximalElement = curArray(curPos)
            } else {
              isEnd = true
            }
          }
        } else {
          count = 1
          maximalElement = curVal
        }
      } else {
        isEnd = true
      }

      currentPosOfArrays(p) = curPos
      p = (p + 1) % numOfArrays
    }

    ArraySegment(buffer.toArray)
  }



  def binaryMergeLikeIntersection(leftArray:ArraySegment, rightArray:ArraySegment):ArraySegment ={

    val buffer = new ArrayBuffer[Int]()

    if (leftArray == null || rightArray == null){
      return ArraySegment(buffer.toArray)
    }

    val lEnd = leftArray.size
    val rEnd = rightArray.size
    var lcur = 0
    var rcur = 0

    while((lcur < lEnd) && (rcur < rEnd)) {
      val lValue = leftArray(lcur)
      val rValue = rightArray(rcur)

      if (lValue < rValue) lcur += 1
      else if (lValue > rValue) rcur += 1
      else {
        buffer += lValue
        lcur += 1
        rcur += 1
        }
      }

    ArraySegment(buffer.toArray)
  }

  def mergeLikeIntersection(arrays:Array[ArraySegment]):ArraySegment ={

    if (arrays.size == 1){
      return arrays(0)
    }
//    assert(arrays.size > 1)

    val sortedArray = arrays.map(f => (f,f.size)).sortBy(_._2).map(_._1)

    if (sortedArray.size == 2){
      return binaryMergeLikeIntersection(sortedArray(0), sortedArray(1))
    } else {
      var i = 2
      var arraysSize = sortedArray.size
      var tmpArray = binaryMergeLikeIntersection(sortedArray(0), sortedArray(1))
      while(i < arraysSize){
        tmpArray = binaryMergeLikeIntersection(tmpArray, sortedArray(i))
        i = i+1
      }


      return tmpArray
      }
    }

}