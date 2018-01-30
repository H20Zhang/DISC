package Plan

import org.apache.spark.Logo.UnderLying.utlis.{ExamplePattern, PointToNumConverter}
import org.scalatest.FunSuite

class ExamplePatternTest extends FunSuite{

  val data = "./wikiV.txt"

//  test("Triangle"){
//    val triangle = new ExamplePattern(data).triangle
//    assert(triangle.size() == 608389)
//  }
//
//  test("ChordalSquare"){
//    val chordalSquare = new ExamplePattern(data).chordalSquareFast
//    assert(chordalSquare.size() == 40544543)
//  }
//
//  test("threeLine"){
//    val threeLine = new ExamplePattern(data).threeLine
//    println("threeLine:" + threeLine.size())
//  }

//  test("trianglePlusTwoEdge"){
//    val trianglePlusTwoEdge = new ExamplePattern(data).trianglePlusTwoEdge
//    println("trianglePlusTwoEdge:" + trianglePlusTwoEdge.size())
//  }
//
//  test("trianglePlusWedge"){
//    val trianglePlusWedge = new ExamplePattern(data).trianglePlusWedge
//    println("trianglePlusWedge:" + trianglePlusWedge.size())
//  }

//  test("house"){
//    val house = new ExamplePattern(data).house
//    assert(house.size() == 9488779111L)
//  }

//  test("houseFast"){
//    val house = new ExamplePattern(data).houseFast
//    assert(house.size() == 9488779111L)
//  }

    test("houseIntersectionFast"){
      val house = new ExamplePattern(data).houseIntersectionFast
      assert(house.size() == 9488779111L)
    }

//
//  test("threeTriangleFast"){
//    val threeTriangle = new ExamplePattern(data).threeTriangleFast
//    assert(threeTriangle.size() == 4105908615L)
//  }

//  test("triangleWithOneEdge"){
//    val triangleWithOneEdge = new ExamplePattern(data).trianglePlusOneEdge
//    println("triangleWithOneEdge:" + triangleWithOneEdge.size())
//  }

//  test("square"){
//        val square = new ExamplePattern(data).square
//        assert(square.size() == 57654491)
//      }

//  test("triangleIntersectionVersion"){
//    val triangle = new ExamplePattern(data).triangleIntersectionVersion
//    assert(triangle.size() == 608389)
//  }

//  test("squareFast"){
//    val square = new ExamplePattern(data).squareFast
//    assert(square.size() == 57654491)
//  }

//    test("squareVerificationFast"){
//      val square = new ExamplePattern(data).squareIntersectionVerificationFast
//      assert(square.size() == 57654491)
//    }
//



  //  test("Pattern"){
//    val threeTriangle = ExamplePattern.threeTriangle
//    assert(threeTriangle.size() == 4105908615L)
//  }




}
