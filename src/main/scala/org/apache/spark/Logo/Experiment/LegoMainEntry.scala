package org.apache.spark.Logo.Experiment

import org.apache.spark.Logo.UnderLying.utlis.{ExamplePattern, SparkSingle}

object LegoMainEntry {


  def main(args: Array[String]): Unit = {
    val data = args(0)
    val triangle = new ExamplePattern(data).triangle
    println(triangle.size())

//    SparkSingle.isCluster = true
//    val house = new ExamplePattern(data).houseFast
//    println(house.size())
//
//    val square = new ExamplePattern(data).squareFast
//    println(square.size())

//    val threeTriangle = new ExamplePattern(data).threeTriangle
//    println(threeTriangle.size())
  }
}
