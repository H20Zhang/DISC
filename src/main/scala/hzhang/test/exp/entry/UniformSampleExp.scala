package hzhang.test.exp.entry

import org.apache.spark.adj.deprecated.execution.sampler.{SquareSampler, TriangleSampler}
import org.apache.spark.adj.utlis.SparkSingle

object UniformSampleExp {
  def main(args: Array[String]): Unit = {
    val data = args(0)
    val k = args(1).toInt
    val samplePattern = args(2)

    SparkSingle.isCluster = true
    SparkSingle.appName = s"Logo-${data}-${samplePattern}"

    val sampler = new TriangleSampler(data)

    samplePattern match {
      case "Triangle" => {
        print(s"samples retrieved: ${sampler.triangleSamplesCount(k)}")
      }
      case "Points" =>{
        println(sampler.selectPoints(k).count())
      }
      case "Wedges" => {
        println(sampler.wedgeSample(k).count())
      }
      case "Square" => {
        val squareSampler = new SquareSampler(data)
        print(s"samples retrieved: ${squareSampler.squareSamplesCount(k)}")
      }
      case "SquareEdge" => {
        val squareSampler = new SquareSampler(data)
        print(squareSampler.selectEdges(k).count())
      }
      case "SquareThreePath" => {
        val squareSampler = new SquareSampler(data)
        print(squareSampler.threePathSample(k).count())
      }
      case "FourClique" => {
        val squareSampler = new SquareSampler(data)
        print(s"samples retrieved: ${squareSampler.fourCliqueSamplesCount(k)}")
      }

    }
  }
}
