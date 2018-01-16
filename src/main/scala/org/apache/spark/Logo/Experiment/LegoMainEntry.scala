package org.apache.spark.Logo.Experiment

import org.apache.spark.Logo.UnderLying.utlis.{ExamplePattern, SparkSingle}

object LegoMainEntry {


  def main(args: Array[String]): Unit = {
    val data = args(0)
//    val triangle = new ExamplePattern(data).triangle
//    println(triangle.size())

    SparkSingle.isCluster = true
    val chordalSquare = new ExamplePattern(data).chordalSquareFast
    println(chordalSquare.size())

//    val threeTriangle = new ExamplePattern(data).threeTriangle
//    println(threeTriangle.size())
  }
}
