package Plan

import org.apache.spark.Logo.UnderLying.utlis.ExamplePattern
import org.scalatest.FunSuite

class ExamplePatternTest extends FunSuite{
  test("Triangle"){
    val triangle = ExamplePattern.triangle
    assert(triangle.size() == 608389)
  }

//  test("Pattern"){
//    val chordalSquare = ExamplePattern.chordalSquare
//    assert(chordalSquare.size() == 40544543)
//  }

//  test("Pattern"){
//        val square = ExamplePattern.square
//        assert(square.size() == 57654491)
//      }

//  test("Pattern"){
//    val threeTriangle = ExamplePattern.threeTriangle
//    assert(threeTriangle.size() == 4105908615L)
//  }


}
