package deprecated.Example

import hzhang.test.exp.entry.StatisticExp
import org.scalatest.FunSuite

class Skewness extends FunSuite{


  test("skewness"){
    StatisticExp.main(Array("./wikiV.txt"))
  }
}
