package Plan

import org.apache.spark.Logo.UnderLying.utlis.{ExamplePattern, ExamplePatternSampler, SparkSingle}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SampledExamplePatternTest extends FunSuite with BeforeAndAfterAll{

  val data = "./wikiV.txt"
  val sampledPattern = new ExamplePatternSampler(data, k=1)
  val pattern = new ExamplePattern(data)

  test("triangleSample"){

    val kList = List(1,2,3,4,5,10,15,20,30)

    val triangleSize = pattern.triangleNew.size()
    val edgeSize = sampledPattern.rawEdge.count()

    for (i <- kList){
      println(s"k is ${i}")
      val sampledPattern = new ExamplePatternSampler(data, k=i)
      val edgeSampledSize = sampledPattern.sampleRawEdge.count()

      println(s"edge: ${edgeSize}, sampledEdge: ${edgeSampledSize}, ratio: ${edgeSampledSize.toDouble / edgeSize}")

      val triangleSampledSize = sampledPattern.triangleSample.size()
      println(s"triangle: ${triangleSize},sampledTriangle: ${triangleSampledSize} ratio:${triangleSampledSize.toDouble / triangleSize}")
    }
  }

  override protected def afterAll(): Unit = {
    SparkSingle.close()
  }
}
