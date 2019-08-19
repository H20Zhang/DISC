package Experiment

import org.apache.spark.adj.execution.rdd.loader.DataLoader
import org.apache.spark.adj.execution.sampler.{SquareSampler, TriangleSampler}
import org.apache.spark.adj.execution.utlis.SparkSingle
import org.scalatest.FunSuite

class UniformSampleTest extends FunSuite{

  val data = "./wikiV.txt"
  test("UniformTriangle"){
    val sampler = new SquareSampler(data)
    sampler.squareSamplesCount(100)
  }

}
