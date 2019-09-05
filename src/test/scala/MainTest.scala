import org.apache.spark.adj.database.{Catalog, Query, RelationSchema}
import org.apache.spark.adj.parser.simpleDml.SimpleParser
import org.apache.spark.adj.utils.misc.SparkSingle
import org.apache.spark.adj.utils.testing.{
  HCubeJoinValidator,
  HCubeTester,
  LeapFrogJoinValidator,
  QueryGenerator
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
    val schemaR3 = RelationSchema("R3", Seq("C", "D"))
    val schemaR4 = RelationSchema("R4", Seq("B", "D"))
    val schemaR5 = RelationSchema("R5", Seq("A", "D"))
    val schemaR6 = RelationSchema("R6", Seq("C", "E"))
    val schemaR7 = RelationSchema("R7", Seq("D", "E"))

    schemaR0.register(dataAdress)
    schemaR1.register(dataAdress)
    schemaR2.register(dataAdress)
    schemaR3.register(dataAdress)
    schemaR4.register(dataAdress)
    schemaR5.register(dataAdress)
    schemaR6.register(dataAdress)
    schemaR7.register(dataAdress)

    val query0 = "Join R3;R6;R7"
    val relation8 = Query.query(query0)
    relation8.schema.register(relation8.rdd)

    //form query
//    val query = "Join R0;R1;R2;R3;R4;R5;R6;R7"
//    Query.countQuery(query)

    //chordalSquare
//    val query0 = "Join R1;R3;R4"
//    val relation8 = Query.query(query0)
//    relation8.schema.register(relation8.content)
//
//    val query = s"Join R0;R2;${relation8.schema.name}"
//    Query.countQuery(query)

    //chordalSquare -- no intermediate
//    val query = s"Join R0;R1;R2;R3;R4"
//    Query.countQuery(query)

    //fourClique
//    val query = s"Join R0;R1;R2;R3;R4;R5"
//    Query.countQuery(query)

    //near5Clique
//    val query = s"Join R0;R1;R2;R3;R4;R5;${relation8.schema.name}"
//    Query.countQuery(query)

    //near5Clique -- no intermediate
    val query = s"Join R0;R1;R2;R3;R4;R5;R6;R7"
    Query.countQuery(query)

    //lolipop
//    val query = s"Join R0;R1;R8"
//    Query.countQuery(query)

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
    import org.apache.spark.adj.utils.extension.SeqUtil._
    println(seq subset ())
  }

  override protected def afterAll(): Unit = {
//    SparkSingle.close()
  }

}
