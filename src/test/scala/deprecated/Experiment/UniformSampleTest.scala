package deprecated.Experiment

import org.apache.spark.adj.deprecated.execution.rdd.loader.DataLoader
import org.apache.spark.adj.deprecated.execution.sampler.{SquareSampler, TriangleSampler}
import org.apache.spark.adj.utils.SparkSingle
import org.scalatest.FunSuite

class UniformSampleTest extends FunSuite{

  val data = "./wikiV.txt"
  test("UniformTriangle"){
    val sampler = new SquareSampler(data)
    sampler.squareSamplesCount(100)
  }

}
