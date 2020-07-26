package disc.integration

import disc.SparkFunSuite
import org.apache.spark.disc.optimization.rule_based.aggregate.ManualRelationDecomposer
import org.apache.spark.disc.testing.{ExpData, ExpQuery}

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
