package org.apache.spark.Logo.Experiment

import org.apache.spark.Logo.Novel.Pregel
import org.apache.spark.Logo.UnderLying.UniformSampler.{SquareSampler, TriangleSampler}
import org.apache.spark.Logo.UnderLying.utlis.SparkSingle

object LegoUniformSample {
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
