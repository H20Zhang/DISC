package adj.optimization

import adj.SparkFunSuite
import org.apache.spark.adj.database.{Catalog, Relation}
import org.apache.spark.adj.optimization.costBased.comp.{
  EnumShareComputer,
  NonLinearShareComputer
}
import org.apache.spark.adj.optimization.stat.Statistic
import org.apache.spark.adj.utils.exp.ExpQuery
import org.apache.spark.adj.utils.testing.QueryGenerator

import scala.util.Random

class ShareComputerTest extends SparkFunSuite {

  val prefix = "./examples/"
  val graphDataAdresses = Map(
    ("eu", "email-Eu-core.txt"),
    ("wikiV", "wikiV.txt"),
    ("debug", "debugData.txt")
  )
  val dataAdress = prefix + graphDataAdresses("wikiV")

  test("EnumShareComputer") {
    val numRelation = 6
    val arity = 4
    val cardinality = 1000
    val query = QueryGenerator.genRandomQuery(numRelation, arity, cardinality)

    val relations = query._1.zip(query._2).map {
      case (content, schema) =>
        val rdd = sc.parallelize(content)
        Relation(schema, rdd)
    }

    val statistic = Statistic.defaultStatistic()
    relations.foreach(relation => statistic.add(relation))

    val numServer = 8
    val shareComputer = new EnumShareComputer(query._2, numServer)
    val catlog = Catalog.defaultCatalog()

    println(s"query:${query._2.zip(query._1).map(f => (f._1, f._2.size))}")
    println(
      s"all plausible share:${shareComputer.genAllShare().map(_.toSeq).size}"
    )

    val optimalShare = shareComputer.optimalShareAndLoadAndCost()
    println(s"optimal share:${optimalShare._1.map(
      f => (catlog.getAttribute(f._1), f._2)
    )}, cost:${optimalShare._2}, load:${optimalShare._3}")
  }

  test("NonLinearShareComptuer") {
    val query = s"triangle"
    val schemas = new ExpQuery(dataAdress) getSchema (query)
    val memoryBudget = 5000
    Random.setSeed(System.currentTimeMillis())
    val cardinalities = schemas.map(f => Random.nextDouble() * 10000)
    val shareComputer =
      new NonLinearShareComputer(schemas, cardinalities, memoryBudget)

    val script = shareComputer.genOctaveScript()
    println(script)
    val optimalShare = shareComputer.optimalShare()
    println(optimalShare)
  }

}
