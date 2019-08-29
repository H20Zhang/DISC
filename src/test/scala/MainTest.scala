import org.apache.spark.adj.database.{Catalog, Query, RelationSchema}
import org.apache.spark.adj.parser.simpleDml.SimpleParser
import org.apache.spark.adj.utils.SparkSingle
import org.apache.spark.adj.utils.testing.{
  HCubeJoinValidator,
  LeapFrogJoinValidator,
  QueryGenerator,
  HCubeTester
}
import org.apache.spark.sql.AnalysisException
import org.scalatest.exceptions.TestFailedException
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.{BitSet, LinearSeq, SortedSet}

class MainTest extends FunSuite with BeforeAndAfterAll {

  val prefix = "./examples/"
  val graphDataAdresses = Map(
    ("eu", "email-Eu-core.txt"),
    ("wikiV", "wikiV.txt"),
    ("debug", "debugData.txt")
  )
  val dataAdress = prefix + graphDataAdresses("wikiV")

  test("main") {
    //register relations
    val schemaR0 = RelationSchema("R0", Seq("A", "B"))
    val schemaR1 = RelationSchema("R1", Seq("B", "C"))
    val schemaR2 = RelationSchema("R2", Seq("C", "A"))

    schemaR0.register(dataAdress)
    schemaR1.register(dataAdress)
    schemaR2.register(dataAdress)

    //form query
    val query = "Join R0;R1;R2"
    Query.countQuery(query)
  }

  test("HCubeJoin") {
    val spark = SparkSingle.getSparkSession()
    val numRelation = 4
    val arity = 4
    val cardinality = 1000
    val testRun = 100

    val tester = new HCubeTester(numRelation, arity, cardinality, testRun)
    tester.testRuns()
  }

  test("scala") {
    val seq = Seq(1, 2, 3, 4).map(f => (f, f))
    val seq2 = seq

    val schemaR2 = RelationSchema("R2", Seq("C", "A"))
    val schemaR0 = RelationSchema("R2", Seq("C", "A"))

    //implicit class
    import org.apache.spark.adj.utils.SeqHelper._
    println(seq subset ())
  }

  override protected def afterAll(): Unit = {
    SparkSingle.close()
  }

}
