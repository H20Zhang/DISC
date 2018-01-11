package graphFrameTest

import graphFrameApplication.{GraphFrameLoader, mainEntry}
import org.apache.spark.Logo.UnderLying.utlis.SparkSingle
import org.graphframes.{GraphFrame, examples}
import org.scalatest.FunSuite

class mainTest extends FunSuite{




//  test("mainEntryTest"){
//
//    val sc = SparkSingle.getSparkContext()
//    val g = GraphFrameLoader.load(sc,"./wikiV.txt")
////    val g = examples.Graphs.friends
//
//
//    val testing = new mainEntry(g)
//    testing.showSchema()
//  }

//  test("schema"){
//
//    val spark = SparkSingle.getSparkSession()
//    // Vertex DataFrame
//    val v = spark.sqlContext.createDataFrame(List(
//      ("a", "Alice", 34),
//      ("b", "Bob", 36),
//      ("c", "Charlie", 30),
//      ("d", "David", 29),
//      ("e", "Esther", 32),
//      ("f", "Fanny", 36),
//      ("g", "Gabby", 60)
//    )).toDF("id", "name", "age")
//    // Edge DataFrame
//    val e = spark.sqlContext.createDataFrame(List(
//      ("a", "b", "friend"),
//      ("b", "c", "follow"),
//      ("c", "b", "follow"),
//      ("f", "c", "follow"),
//      ("e", "f", "follow"),
//      ("e", "d", "friend"),
//      ("d", "a", "friend"),
//      ("a", "e", "friend")
//    )).toDF("src", "dst", "relationship")
//    // Create a GraphFrame
//    val g = GraphFrame(v, e)
//
//        val testing = new mainEntry(g)
//        testing.showSchema()
//
//
//  }
}
