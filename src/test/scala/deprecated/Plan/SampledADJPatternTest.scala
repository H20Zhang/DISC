package deprecated.Plan


import hzhang.test.exp.utils.{ADJPattern, ExamplePatternSampler}
import org.apache.spark.adj.utils.SparkSingle
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SampledADJPatternTest extends FunSuite with BeforeAndAfterAll{

  val data = "./wikiV.txt"
  val sampledPattern = new ExamplePatternSampler(data, k=1)
  val pattern = new ADJPattern(data)

  test("triangleSample"){

//    val kList = List(1,2,3,4,5,10,15,20,30)
    val kList = List(1)
    val query = List("wedge","triangle","chordalSquare","square").toParArray
    val realSizes = query.map{f =>

      1
//      pattern.pattern(f).size()
    }

    kList.foreach{
      i => {
        println(s"k is ${i}")
        val sampledPattern = new ExamplePatternSampler(data, k=i)

        println(s"edge: ${sampledPattern.rawEdgeSize}, sampledEdge: ${sampledPattern.sampledRawEdgeSize}, ratio: ${sampledPattern.sampledRawEdgeSize.toDouble / sampledPattern.rawEdgeSize}")
        query.zip(realSizes).map{
          f =>
            val sampledSize = sampledPattern.pattern(f._1)
            val realSize = f._2
//            println(s"sampledSize:${sampledSize} realSize:${realSize} Ratio:${sampledSize / realSize}")
        }
      }
    }

  }

  override protected def afterAll(): Unit = {
    SparkSingle.close()
  }
}
