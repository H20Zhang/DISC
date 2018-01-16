package Plan

import org.apache.spark.Logo.UnderLying.utlis.{ExamplePattern, PointToNumConverter}
import org.scalatest.FunSuite

class ExamplePatternTest extends FunSuite{

  val data = "./wikiV.txt"

//  test("Triangle"){
//    val triangle = new ExamplePattern(data).triangle
//    assert(triangle.size() == 608389)
//  }




  test("ChordalSquare"){
    val chordalSquare = new ExamplePattern(data).chordalSquareFast
    assert(chordalSquare.size() == 40544543)
  }

//  test("square"){
//        val square = new ExamplePattern(data).square
//        assert(square.size() == 57654491)
//      }

//  test("Pattern"){
//    val threeTriangle = ExamplePattern.threeTriangle
//    assert(threeTriangle.size() == 4105908615L)
//  }


}
