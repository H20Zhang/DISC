package Underlying

import org.scalatest.{BeforeAndAfterAll, FunSuite}

class GraphFrameTest extends FunSuite with BeforeAndAfterAll{



//  test("graphframetest"){
//
//
//
//    val spark = SparkSession.builder().master("local[4]").appName("spark sql example").config("spark.some.config.option", "some-value")
//      .getOrCreate()
//
//    val g: GraphFrame = examples.Graphs.friends  // get example graph
//
//    // Search for pairs of vertices with edges in both directions between them.
//    val motifs: DataFrame = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
////    motifs.explain(true)
//    motifs.show()
//
//    // More complex queries can be expressed by applying filters.
////    motifs.filter("b.age > 30").explain(true)
//
//
//
//
//    val chain4 = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")
//
//    // Query on sequence, with state (cnt)
//    //  (a) Define method for updating state given the next element of the motif.
//    def sumFriends(cnt: Column, relationship: Column): Column = {
//      when(relationship === "friend", cnt + 1).otherwise(cnt)
//    }
//    //  (b) Use sequence operation to apply method to sequence of elements in motif.
//    //      In this case, the elements are the 3 edges.
//    val condition = Seq("ab", "bc", "cd").
//      foldLeft(lit(0))((cnt, e) => sumFriends(cnt, col(e)("relationship")))
//    //  (c) Apply filter to DataFrame.
//    val chainWith2Friends2 = chain4.where(condition >= 2).groupBy("a").count().show()
//
//
//    spark.close()
//  }
//
//  override protected def afterAll(): Unit = {
//
//  }




}
