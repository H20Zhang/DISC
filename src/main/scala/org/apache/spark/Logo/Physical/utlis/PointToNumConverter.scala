package org.apache.spark.Logo.Physical.utlis

import scala.collection.mutable.ArrayBuffer



//convert a point to a number
// point(1,2,3,4) represent number 4,3,2,1
class PointToNumConverter(val parts:Seq[Int]){

  //points(4,5)
  //parts(20,10)

  //we assume here that List(0,1,2,3) represent number 3,2,1,0
  //convert a x variable based number to 10 based system
  def convertToNum(point:Seq[Int]) = point.reverse.zipWithIndex.map{f =>

    val multipliers = parts.dropRight(f._2+1)

    if (multipliers.isEmpty){
      f._1
    }else{
      f._1*multipliers.product
    }
    }.sum

  //convert a 10 based system number to x varible based
  def NumToList(index:Int) = {
    val buffer = new ArrayBuffer[Int]()
    var total = index

    parts.drop(1).foldRight(buffer){case(mod, buffer) =>
      buffer += (total % mod)
      total = total/mod
      buffer
    }

    buffer += total
    buffer.toList
  }
}
