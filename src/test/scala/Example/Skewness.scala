package Example

import org.apache.spark.adj.exp.entry.StatisticExp
import org.scalatest.FunSuite

class Skewness extends FunSuite{


  test("skewness"){
    StatisticExp.main(Array("./wikiV.txt"))
  }
}
