package org.apache.spark.Logo.Plan.LogicalPlan.HyberCubeOptimize

import scala.collection.mutable.ArrayBuffer


/**
* @param PatternSize: Array((Array:The Attribute Pattern has using number representation,Int:Size of the Pattern))
* @param minP: min amount of subtasks generated
* @param maxP: max amount of subtasks generated
* @param length: number of attributes
*/
class HyberCubeOptimizer(PatternSize:Array[(Array[Int],Int)], minP:Int, maxP:Int, length:Int) {


  val pGenerator = new PGenerator(maxP,length, p = { f => f.product > minP})


  def allPlans() = detailAllPlans().map(f => PPlan(f._1, f._1.product, f._3))

  //output Array(attributes of pattern, size of pattern, totalSize, number of repitition)
  def detailAllPlans() = {
    val Ps = pGenerator.generateAllP()
    val complementryPatternSize = PatternSize.map{f =>
      val tempArray = new ArrayBuffer[Int]()
      for (i <- 0 until length){
        if (!f._1.contains(i)){
          tempArray += i
        }
      }

      (f._1,f._2,tempArray)
    }

    Ps.map{f =>
      val totalSizes = complementryPatternSize.map{h =>
        var res = 1
        for (i <- h._3){
          res = res * f(i)
        }
        (h._1,h._2,res*h._2,res)
      }

      val totalSize = totalSizes.map(f => f._3).sum
      (f,totalSizes,totalSize)
    }
  }


}

case class PPlan(P:Seq[Int], subTasks:Int, totalCost:Int)
