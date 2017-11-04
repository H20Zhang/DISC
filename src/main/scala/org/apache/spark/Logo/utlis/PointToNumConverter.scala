package org.apache.spark.Logo.utlis

import scala.collection.mutable.ArrayBuffer

class PointToNumConverter(val parts:List[Int]){

  //convert a x variable based number to 10 based system
  def convertToNum(point:List[Int]) = point.zipWithIndex.map{f =>

    val mutipliers = parts.drop(f._2+1)

    if (mutipliers.isEmpty){
      f._1
    }else{
      f._1*parts.drop(f._2+1).product
    }
    }.sum

  //convert a 10 based system number to x varible based
  def NumToList(index:Int) = {
    val buffer = new ArrayBuffer[Int]()
    var total = index

    parts.dropRight(1).foldLeft(buffer){case(buffer, mod) =>
      buffer += (total % mod)
      total = total/mod
      buffer
    }

    buffer += total
    buffer.toList
  }
}
