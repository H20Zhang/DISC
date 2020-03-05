package dsce

import adj.SparkFunSuite
import org.apache.spark.dsce.optimization.aggregate.ManualRelationDecomposer
import org.apache.spark.dsce.testing.{ExpData, ExpQuery}
import org.scalatest.FunSuite

class ManualRelationDecomposerTest extends SparkFunSuite {

//  val queries = Seq("quadTriangle")
  val queries = Seq("quadTriangle")
  val expQuery = new ExpQuery(ExpData.getDataAddress("debug"))

  test("stringToGHD") {

    println(s"stringToGHD")

    queries.foreach { q =>
      val decomposer = new ManualRelationDecomposer(expQuery.getSchema(q))
      val ghd = decomposer.stringToGHD(decomposer.quadTriangleGHD)
      println(s"ghd:$ghd")
    }
  }

  test("stringToGraph") {

    println(s"stringToGHD")

    queries.foreach { q =>
      val decomposer = new ManualRelationDecomposer(expQuery.getSchema(q))
      val graph = decomposer.stringToGraph(decomposer.quadTriangleGraph)
      println(s"Graph(V:${graph.V}, E:${graph.E})")
    }
  }

  test("GHD") {
    println(s"GHD")

    queries.foreach { q =>
      val decomposer = new ManualRelationDecomposer(expQuery.getSchema(q))
      val ghd = decomposer.decomposeTree().head
      println(s"GHD:$ghd")
    }

  }

}
