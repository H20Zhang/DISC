package Plan

import org.apache.spark.Logo.Plan.LogicalPlan.Decrapted.TestParser
import org.scalatest.FunSuite

class QueryParserTest extends FunSuite{


  test("testParser"){
    val parser = new TestParser

    val result = parser.parseAll(parser.edges,"AB->CD; CD->FE;")
    println(result)

  }



}
