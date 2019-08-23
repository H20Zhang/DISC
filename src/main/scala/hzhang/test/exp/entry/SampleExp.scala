package hzhang.test.exp.entry


import hzhang.test.exp.utils.{ADJPattern, ExamplePatternSampler}
import org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.Decrapted.CostBasedOptimizer._
import org.apache.spark.adj.utlis.SparkSingle





object SampleExp {


  def testPlans(args:Array[String]) = {

    val data = args(0)
    val patternName = args(1)
    val h = args(2).toInt
    val k = args(3).toDouble
    val k2 = args(4).toInt

    SparkSingle.isCluster = true
    SparkSingle.appName = s"SAMPLE:data:${data}-patternName:${patternName}-h:${h}-k:${k}-k2:${k2}"

    val memCoefficient = List(1)
    val memLimits = memCoefficient.map(Math.pow(2,_))


    patternName match {
      case "houseQuery" => {
        val houseEstimator = new HouseCostEstimator(data,h,k, k2)
        memLimits.map{f =>
          val housePlans = houseEstimator.generatePlans(f)

          println(s"memLimit is ${f}*10^6")
          println("House deprecated.Plan")

          houseEstimator.informMaps.foreach(println)
        }
      }
      case "near5CliqueQuery" => {
        val near5CliqueEstimator = new near5CliqueCostEstimator(data,h,k, k2)

        memLimits.map{f =>
          val near5CliquePlans = near5CliqueEstimator.generatePlans(f)

          println()
          println("Near5Clique deprecated.Plan")
          near5CliqueEstimator.informMaps.foreach(println)
        }
      }

      case "chordalSquareQuery" => {
        val chordalSquareEstimator = new ChordalSquareCostEstimator(data,h,k, k2)

        memLimits.map{f =>
//          val near5CliquePlans = chordalSquareEstimator.generatePlans()

          println()
          println("ChordalSquare")
          chordalSquareEstimator.informMaps.foreach(println)
        }
      }

      case "ThreeTriangleQuery" => {
        val threeTriangleEstimator = new ThreeTriangleCostEstimator(data,h,k, k2)

        memLimits.map{f =>
//          val near5CliquePlans = threeTriangleEstimator.generatePlans()

          println()
          println("ThreeTriangle")
          threeTriangleEstimator.informMaps.foreach(println)
        }
      }
    }




  }


  def testIndividual(args:Array[String]): Unit ={
    val data = args(0)
    val patternName = args(1)
    val h = args(2).toInt
    val k = args(3).toDouble
    val k2 = args(4).toInt

    SparkSingle.isCluster = true
    SparkSingle.appName = s"SAMPLE:data:${data}-patternName:${patternName}-h:${h}-k:${k}"

    val pattern = new ADJPattern(data)

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
            val start_time = System.nanoTime()
            val time_size_pair = sampledPattern.time_size()
            val sampledSize = (time_size_pair._1).toLong

            val end_time = System.nanoTime()

            (f,sampledSize,time_size_pair._2,end_time-start_time, time_size_pair._1)
        }.foreach(f => println(s"SResults:${f._1}:sampledSize:${f._5}:estimatedSize:${f._2}:sampleTime:${f._3} ms"))

//        println(s"edge: ${rawEdgeSize/base},, sampledEdge: ${sampledRawEdgeSize/base}, ratio: ${ratio}")
      }
    }
  }


  def main(args: Array[String]): Unit = {
    testIndividual(args)
//    testPlans(args)
  }
}
