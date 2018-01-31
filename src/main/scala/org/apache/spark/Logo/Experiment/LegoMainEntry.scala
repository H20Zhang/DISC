package org.apache.spark.Logo.Experiment

import org.apache.spark.Logo.UnderLying.utlis.{ExamplePattern, SparkSingle}

object LegoMainEntry {


  def main(args: Array[String]): Unit = {
    val data = args(0)
    val algorithm = args(1)


    SparkSingle.isCluster = true
    algorithm match {
      case "triangle" => val triangle = new ExamplePattern(data).triangleIntersectionVersion; println(triangle.size())
      case "house" => val house = new ExamplePattern(data).houseIntersectionFast; println(s"house:" + house.size())
      case "chordalSquare" => val chordalSquare = new ExamplePattern(data).chordalSquareFast; println(chordalSquare.size())
      case "square" => val square = new ExamplePattern(data).squareIntersectionVerificationFast; println(square.size())
      case "threeTriangle" => val threeTriangle = new ExamplePattern(data).threeTriangleFast; println(threeTriangle.size())
      case "trianglePlusOneEdge" => val trianglePlusOneEdge = new ExamplePattern(data).trianglePlusOneEdge; println(trianglePlusOneEdge.size())
      case "trianglePlusTwoEdge" => val trianglePlusTwoEdge = new ExamplePattern(data).trianglePlusTwoEdge; println(trianglePlusTwoEdge.size())
      case "trianglePlusWedge" => val trianglePlusWedge = new ExamplePattern(data).trianglePlusWedge; println(trianglePlusWedge.size())
    }


    //    val chordalSquare = new ExamplePattern(data).chordalSquareFast
    //    println(chordalSquare.size())


  }
}
