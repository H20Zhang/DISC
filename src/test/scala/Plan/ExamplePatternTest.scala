package Plan

import org.apache.spark.Logo.UnderLying.dataStructure.ValuePatternInstance
import org.apache.spark.Logo.UnderLying.utlis.{ExamplePattern, PointToNumConverter, SparkSingle}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


class ExamplePatternTest extends FunSuite with BeforeAndAfterAll{

  val data = "./wikiV.txt"
//  val data = "./email-Eu-core.txt"
//  val queries = List("houseF", "threeTriangleF", "trianglePlusOneEdge")
  val queries = List("debug")
//  val queries = List("squarePlusOneEdgeF", "trianglePlusTwoEdgeF")
//val queries = List("trianglePlusTwoEdgeF")

//  val queries = List("threeTriangleF")
//val queries = List("square", "triangle", "chordalSquare", "house", "threeTriangle")
  val sizeReference = List(("debug",1L), ("trianglePlusOneEdge",1L),("trianglePlusTwoEdgeF",1),("squarePlusOneEdgeF",1),("square",57654491L),("triangle",608389L),("chordalSquare",40544543L),("houseF",2365994715L),("house",9488779111L),("threeTriangle",4105908615L),("threeTriangleF",2106389L)).toMap


  test("Pattern"){
    SparkSingle.appName = s"Logo-${data}"
    val pattern = new ExamplePattern(data)
    queries.foreach{
      f =>
        println(s"execute $f")
//        assert(pattern.pattern(f).size() == sizeReference(f))
//        println(pattern.pattern(f).rdd().map(f => ((f.getValue(0),f.getValue(1)),1)).reduceByKey(_ + _).count())




        pattern.pattern(f).rdd().mapPartitions({
          f =>

            val longMap = new mutable.LongMap[Int]()
            while (f.hasNext){
              val p = f.next()
              val key = (p(0).toLong << 32) | (p(1) & 0xffffffffL)

              val v = longMap.getOrNull(key)
              if (v != null) {
                longMap.update(key,v+1)
              } else {
                longMap.put(key, 1)
              }
            }
            longMap.iterator
        },true).reduceByKey(_ + _).count()


    }
  }

//  test("testAggregation"){
//        val pattern = new ExamplePattern(data)
////        val x = pattern.houseIntersectionFast.rdd().count()
////    val z = pattern.houseIntersectionFast.rdd().count()
//
////    val triangle = pattern.triangleIntersectionVersion.rdd()
//
////    println(triangle.countApprox(100))
//
//
//    val theList = pattern.edge.rdd().flatMap(f => Iterable(f.getValue(0),f.getValue(1))).distinct().collect()
//    val theMap = theList.map(f => (f, Random.nextInt() % 10)).toMap
//    val y = pattern.squareIntersectionVerificationFast.rdd().filter(f => (theMap(f.getValue(0)) == theMap(f.getValue(1)))).map(f => ((f.getValue(0),f.getValue(1)),1)).reduceByKey(_ + _).max()(new Ordering[((Int,Int),Int)]() {
//        override def compare(x: ((Int,Int),Int), y: ((Int,Int),Int)): Int =
//        Ordering[Int].compare(x._2, y._2)
//})
//    println(y)
////    y.foreach(println)
////    println("size is" + z)
//  }

//  test("testHand"){
//    val pattern = new ExamplePattern(data)
//    pattern.houseHand
//  }

//  test("Triangle"){
//    val triangle = new ExamplePattern(data).triangle
//    assert(triangle.size() == 608389)
//  }
//
//  test("ChordalSquare"){
//    val chordalSquare = new ExamplePattern(data).chordalSquareFast
//    assert(chordalSquare.size() == 40544543)
//  }
//
//  test("threeLine"){
//    val threeLine = new ExamplePattern(data).threeLine
//    println("threeLine:" + threeLine.size())
//  }

//  test("trianglePlusTwoEdge"){
//    val trianglePlusTwoEdge = new ExamplePattern(data).trianglePlusTwoEdge
//    println("trianglePlusTwoEdge:" + trianglePlusTwoEdge.size())
//  }
//
//  test("trianglePlusWedge"){
//    val trianglePlusWedge = new ExamplePattern(data).trianglePlusWedge
//    println("trianglePlusWedge:" + trianglePlusWedge.size())
//  }

//  test("house"){
//    val house = new ExamplePattern(data).house
//    assert(house.size() == 9488779111L)
//  }

//  test("houseFast"){
//    val house = new ExamplePattern(data).houseFast
//    assert(house.size() == 9488779111L)
//  }

//    test("houseIntersectionFast"){
//      val house = new ExamplePattern(data).houseIntersectionFast
//      assert(house.size() == 9488779111L)
//    }


//  test("threeTriangleFast"){
//    val threeTriangle = new ExamplePattern(data).threeTriangleFast
//    assert(threeTriangle.size() == 4105908615L)
//  }

//  test("threeTriangleFilterFast"){
//    val threeTriangle = new ExamplePattern(data).threeTriangleFilterFast
//    assert(threeTriangle.size() == 4105908615L)
//  }

//  test("houseIntersectionFilterFast"){
//    val threeTriangle = new ExamplePattern(data).houseIntersectionFilterFast
//    assert(threeTriangle.size() == 4105908615L)
//  }

//  test("triangleWithOneEdge"){
//    val triangleWithOneEdge = new ExamplePattern(data).trianglePlusOneEdge
//    println("triangleWithOneEdge:" + triangleWithOneEdge.size())
//  }

//  test("square"){
//        val square = new ExamplePattern(data).square
//        assert(square.size() == 57654491)
//      }

//  test("triangleIntersectionVersion"){
//    val triangle = new ExamplePattern(data).triangleIntersectionVersion
//    assert(triangle.size() == 608389)
//  }

//  test("squareFast"){
//    val square = new ExamplePattern(data).squareFast
//    assert(square.size() == 57654491)
//  }

//    test("squareVerificationFast"){
//      val square = new ExamplePattern(data).squareIntersectionVerificationFast
//      assert(square.size() == 57654491)
//    }
//



  //  test("Pattern"){
//    val threeTriangle = ExamplePattern.threeTriangle
//    assert(threeTriangle.size() == 4105908615L)
//  }


  override protected def afterAll(): Unit = {
    SparkSingle.close()
  }

}
