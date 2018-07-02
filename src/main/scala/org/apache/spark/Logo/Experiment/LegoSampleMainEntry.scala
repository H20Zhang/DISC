package org.apache.spark.Logo.Experiment

import org.apache.spark.Logo.Plan.LogicalPlan.CostBasedOptimizer._
import org.apache.spark.Logo.UnderLying.utlis.{ExamplePattern, ExamplePatternSampler, SparkSingle}





object LegoSampleMainEntry {


  def testPlans(args:Array[String]) = {

    val data = args(0)
    val patternName = args(1)
    val h = args(2).toInt
    val k = args(3).toInt
    val k2 = args(4).toInt


    val memCoefficient = List(1,2,3,4,5,6,7,8)
    val memLimits = memCoefficient.map(Math.pow(2,_))

    val houseEstimator = new HouseCostEstimator(data,h,k, k2)
    val near5CliqueEstimator = new near5CliqueCostEstimator(data,h,k, k2)

    memLimits.map{f =>
      val housePlans = houseEstimator.generatePlans(f)
      val near5CliquePlans = near5CliqueEstimator.generatePlans(f)

      println(s"memLimit is ${f}*10^6")
      println("House Plan")
      housePlans.foreach(println)

      println()
      println("Near5Clique Plan")
      near5CliquePlans.foreach(println)
    }

  }


  def testIndividual(args:Array[String]): Unit ={
    val data = args(0)
    val patternName = args(1)
    val h = args(2).toInt
    val k = args(3).toInt
    val k2 = args(4).toInt

    SparkSingle.isCluster = true
    SparkSingle.appName = s"SAMPLE:data:${data}-patternName:${patternName}-h:${h}-k:${k}"

    val pattern = new ExamplePattern(data)

    val kList = List(k)

    var query = List(patternName)
    if (patternName == "all"){
      query = List("triangle", "chordalSquare", "square", "fourClique", "squareTriangle", "fourCliqueTriangle", "triangleSquare", "triangleFourClique")
    }

    kList.foreach{
      i => {
        val base = i
        println(s"k is ${i}")
        val sampledPatterns = new ExamplePatternSampler(data,h,h,i, k2)
        val rawEdgeSize = sampledPatterns.rawEdgeSize
        val sampledRawEdgeSize = sampledPatterns.sampledRawEdgeSize
        val ratio = sampledRawEdgeSize.toDouble / rawEdgeSize

        query.map{
          f =>
            val sampledPattern = sampledPatterns.pattern(f)
            val start_time = System.currentTimeMillis()
            val time_size_pair = sampledPattern.time_size()
            val sampledSize = (time_size_pair._1 / ratio).toLong

            val end_time = System.currentTimeMillis()

            (f,sampledSize,time_size_pair._2,end_time-start_time, time_size_pair._1)
        }.foreach(f => println(s"${f._1}:sampledSize:${f._5/base}:estimatedSize:${f._2/base}:sampleTime:${f._3} in one Block :timeUsed:${f._4}"))

        println(s"edge: ${rawEdgeSize/base},, sampledEdge: ${sampledRawEdgeSize/base}, ratio: ${ratio}")
      }
    }
  }


  def main(args: Array[String]): Unit = {
//    testIndividual(args)
    testPlans(args)
  }
}
