package org.apache.spark.adj.plan.deprecated.LogicalPlan.Decrapted.HyberCubeOptimize

import scala.collection.mutable.ArrayBuffer


/**
* @param PatternSize: Array((Array:The Attribute Pattern has using number representation,Int:Size of the Pattern))
* @param minP: min amount of subtasks generated
* @param maxP: max amount of subtasks generated
* @param length: number of attributes
*/
class HyberCubeOptimizer(PatternSize:Array[(Array[Int],Double)], minP:Int, maxP:Int, length:Int, load:Double) {


  val pGenerator = new PGenerator(maxP,length, p = { f => f.product > minP})


  def allPlans() = detailAllPlans().map(f => PPlan(f._1, f._1.product, f._3, f._4)).filter(p => p.Load < load)

  //output Array(attributes of pattern, size of pattern, totalSize, number of repitition)
  def detailAllPlans() = {
    val Ps = pGenerator.generateAllP()
    val complementryPatternSize = PatternSize.map{f =>
      val outsideAttributes = new ArrayBuffer[Int]()
      for (i <- 0 until length){
        if (!f._1.contains(i)){
          outsideAttributes += i
        }
      }

      (f._1,f._2,outsideAttributes)
    }

    Ps.map{f =>
      val relationInfo = complementryPatternSize.map{ h =>
        var repetition = 1
        for (i <- h._3){
          repetition = repetition * f(i)
        }

        var loadDivision = 1
        for (i <- h._1){
          loadDivision = loadDivision * f(i)
        }
        (h._1,h._2,repetition*h._2,h._2/loadDivision)
      }

      val totalSize = relationInfo.map(f => f._3).sum
      val totalLoad = relationInfo.map(f => f._4).sum
      (f,relationInfo,totalSize, totalLoad)
    }
  }


}

case class PPlan(P:Seq[Int], subTasks:Int, totalCost:Double, Load:Double)
