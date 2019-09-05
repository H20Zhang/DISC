package optimization

import org.apache.spark.adj.database.{Catalog, Relation}
import org.apache.spark.adj.optimization.comp.EnumShareComputer
import org.apache.spark.adj.optimization.stat.Statistic
import org.apache.spark.adj.utils.testing.QueryGenerator
import utils.SparkFunSuite

class ShareComputerTest extends SparkFunSuite {

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

    val optimalShare = shareComputer.optimalShareAndLoadAndCost(numServer)
    println(s"optimal share:${optimalShare._1.map(
      f => (catlog.getAttribute(f._1), f._2)
    )}, cost:${optimalShare._2}, load:${optimalShare._3}")
  }

}
