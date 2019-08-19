package org.apache.spark.adj.execution.utlis

import java.util
import java.util

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Alg {

  def binarySearch[T](array: Array[T], value: T)(implicit arithmetic: Numeric[T]): Int =
    BSearch.search(array, value)

  def binarySearch(array: Array[Int], value: Int, _left:Int, _right:Int)(implicit arithmetic: Numeric[Int]): Int =
    BSearch(array,value,_left, _right)

  def mergelikeIntersection(arrays:Array[Array[Int]]):Array[Int] = Intersection.mergeLikeIntersection(arrays)

  def leapfrogIntersection(arrays:Array[Array[Int]]):Array[Int] = Intersection.leapfrogIntersection(arrays)
}

object BSearch {
  def search[T](array: Array[T], value: T)(implicit arithmetic: Numeric[T]): Int = {
    var left: Int = 0;
    var right: Int = array.length - 1;
    while (right > left) {
      val mid = left + (right - left) / 2
      val comp = arithmetic.compare(array(mid), value)
      if (comp == 0)
        return mid; //negative if test < value
      else if (comp > 0) //list(mid) > value
        right = mid - 1;
      else if (comp < 0) //list(mid) < value
        left = mid + 1;
    }
    -1;
  }

  def search(array: Array[Int], value: Int, _left:Int, _right:Int)(implicit arithmetic: Numeric[Int]): Int = {
    var left: Int = _left;
    var right: Int = _right;
    while (right > left) {
      val mid = left + (right - left) / 2
      val comp = arithmetic.compare(array(mid), value)
      if (comp == 0)
        return mid; //negative if test < value
      else if (comp > 0) //list(mid) > value
        right = mid - 1;
      else if (comp < 0) //list(mid) < value
        left = mid + 1;
    }
    -1;
  }
}

object Intersection {
  //  find the position i where array(i) >= value and i is the minimal value
  //  noted: the input array should be sorted
  private def seek(array: Array[Int], value: Int, _left:Int): Int = {
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

  def leapfrogIntersection(arrays:Array[Array[Int]]):Array[Int] = {

//    check some preliminary conditions
    assert(arrays.size > 1)

    val buffer = new ArrayBuffer[Int]()

    var i = 0
    while(i < arrays.size){
      if (arrays(i) == null){
        return buffer.toArray
      }
      i = i+1
    }

//    find maximum element at the first position
    val numOfArrays = arrays.size
    var maximalElement = Int.MinValue

    i = 0
    while (i < numOfArrays){
      val curVal = arrays(i)(0)
      if (curVal > maximalElement){
        maximalElement = curVal
      }
      i = i + 1
    }

//    intersect the arrays
    var isEnd = false
    var currentPosOfArrays = new Array[Int](numOfArrays)

    var p = 0
    var count = 0

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

//          println(s"curVal:${curVal}, maximalElement:${curVal}, pos:${pos}, array:${p}, count:${count}")

          count += 1
          if (count == numOfArrays) {
            count = 0
            buffer += maximalElement

            curPos += 1
            if (curPos < curArray.size) {
              maximalElement = curArray(curPos)
            } else {
              isEnd = true
            }
          }
        } else {
          count = 0
          maximalElement = curVal
        }
      } else {
        isEnd = true
      }

      currentPosOfArrays(p) = curPos
      p = (p + 1) % numOfArrays
    }

    buffer.toArray
  }

  def binaryMergeLikeIntersection(leftArray:Array[Int], rightArray:Array[Int]):Array[Int] ={

    val buffer = new ArrayBuffer[Int]()

    if (leftArray == null || rightArray == null){
      return buffer.toArray
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

    buffer.toArray
  }

  def mergeLikeIntersection(arrays:Array[Array[Int]]):Array[Int] ={
      assert(arrays.size > 1)

    if (arrays.size == 2){
      return binaryMergeLikeIntersection(arrays(0), arrays(1))
    } else {
      var i = 2
      var arraysSize = arrays.size
      var tmpArray = binaryMergeLikeIntersection(arrays(0), arrays(1))
      while(i < arraysSize){
        tmpArray = binaryMergeLikeIntersection(tmpArray, arrays(i))
        i = i+1
      }

      return tmpArray
      }
    }

}