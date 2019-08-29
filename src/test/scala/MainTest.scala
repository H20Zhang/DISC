import org.apache.spark.adj.database.{Query, RelationSchema}
import org.apache.spark.adj.parser.simpleDml.SimpleParser
import org.apache.spark.adj.utils.SparkSingle
import org.scalatest.FunSuite

class MainTest extends FunSuite{

  val prefix = "./examples/"
  val graphDataAdresses = Map(("eu","email-Eu-core.txt"), ("wikiV", "wikiV.txt"), ("debug", "debugData.txt"))
  val dataAdress = prefix + graphDataAdresses("wikiV")

  test("main"){


    //TODO: there is still some strange behavior when changing attribute order of relation, debug needed
    //register relations
    val schemaR0 = RelationSchema("R0", Seq("A", "B"))
    val schemaR1 = RelationSchema("R1", Seq("B", "C"))
    val schemaR2 = RelationSchema("R2", Seq("C", "A"))
//    val schemaR3 = RelationSchema("R3", Seq("D", "A"))

    schemaR0.register(dataAdress)
    schemaR1.register(dataAdress)
    schemaR2.register(dataAdress)
//    schemaR3.register(dataAdress)


    //form query
//    val parser = new SimpleParser
    val query ="Join R0;R1;R2"
    Query.countQuery(query)
//    val plan = parser.parseDml(query)
//    println(s"unoptimized logical plan:${plan}")
//
//    //optimize plan
//    val optimizedPlan = plan.optimizedPlan()
//    println(s"optimized logical plan:${optimizedPlan}")
//
//    //convert to physical plan
//    val phyiscalPlan = optimizedPlan.phyiscalPlan()
//    println(s"phyiscal plan:${phyiscalPlan}")
//
//    //execute physical plan
//    val outputSize = phyiscalPlan.count()
//    println(s"output relation size:${outputSize}")

    SparkSingle.close()
  }
}
