package Experiment

import org.apache.spark.Logo.UnderLying.Loader.EdgeLoader
import org.apache.spark.Logo.UnderLying.UniformSampler.{SquareSampler, TriangleSampler}
import org.apache.spark.Logo.UnderLying.utlis.SparkSingle
import org.scalatest.FunSuite

class UniformSampleTest extends FunSuite{

  val data = "./wikiV.txt"
  test("UniformTriangle"){
    val sampler = new SquareSampler(data)
    sampler.squareSamplesCount(100)
  }

}
