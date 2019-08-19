package Novel

import hzhang.framework.test.Novel.SparkMLUsage
import org.scalatest.FunSuite

class MLTest extends FunSuite{

  val ml = new SparkMLUsage

  test("logisticRegression"){
    ml.testLogisticalRegression()
//    ml.testDataStructure()
//    ml.testBayes()
  }

}
