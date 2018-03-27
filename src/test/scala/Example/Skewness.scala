package Example

import org.apache.spark.Logo.Experiment.GraphStatistic
import org.scalatest.FunSuite

class Skewness extends FunSuite{


  test("skewness"){
    GraphStatistic.main(Array("./wikiV.txt"))
  }
}
