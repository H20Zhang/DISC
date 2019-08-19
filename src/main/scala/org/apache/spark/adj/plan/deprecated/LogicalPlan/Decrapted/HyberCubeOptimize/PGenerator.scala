package org.apache.spark.adj.plan.deprecated.LogicalPlan.Decrapted.HyberCubeOptimize

import scala.collection.mutable.ArrayBuffer

class PGenerator(totalP:Int,length:Int, p:Array[Int] => Boolean) {

  def generateAllP():ArrayBuffer[Array[Int]] = {
    _generateAllP(1,length).filter(p)
  }

  private def _generateAllP(prevProd:Int,remainLength:Int):ArrayBuffer[Array[Int]] = {


    val largest_possible = totalP / prevProd

    if (remainLength == 1) {
      val mutableArray = new ArrayBuffer[Array[Int]]()
      for (i <- 1 to largest_possible){
        mutableArray += Array(i)
      }

      return mutableArray
    } else {
      val mutableArray = new ArrayBuffer[Array[Int]]()
      for (i <- 1 to largest_possible){
        val subs = _generateAllP(prevProd * i, remainLength - 1)
        for (j <- subs) {
          val tempArray = new Array[Int](remainLength)
          j.copyToArray(tempArray)
          tempArray(remainLength-1) = i
          mutableArray += tempArray
        }
      }
      return mutableArray
    }

  }





}
