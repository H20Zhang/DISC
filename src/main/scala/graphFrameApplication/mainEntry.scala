package graphFrameApplication

import graphFrameApplication.patternMatching.UnanchoredPattern
import org.apache.spark.Logo.UnderLying.utlis.SparkSingle
import org.apache.spark.sql.SparkSession
import org.graphframes.{GraphFrame, examples}

class mainEntry(g:GraphFrame) {

  def runTest(): Unit ={
    val unachoredPatternTest = new UnanchoredPattern(g)
    unachoredPatternTest.test()
  }

  def showSchema(): Unit ={
    val unachoredPatternTest = new UnanchoredPattern(g)
    unachoredPatternTest.showSchema()
  }

}


object mainEntry{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("yarn").appName("GraphFrameTest").config("spark.some.config.option", "some-value")
      .getOrCreate()
    val g = examples.Graphs.friends

    //    val g =GraphFrameLoader.load(spark.sparkContext,args(0))
    val testing = new mainEntry(g)
    testing.runTest()
  }
}
