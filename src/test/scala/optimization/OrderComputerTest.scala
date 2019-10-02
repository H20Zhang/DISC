package optimization

import org.apache.spark.adj.database.Relation
import org.apache.spark.adj.optimization.costBased.comp.OrderComputer
import org.apache.spark.adj.optimization.stat.Statistic
import org.apache.spark.adj.utils.testing.QueryGenerator
import org.scalatest.FunSuite
import utils.SparkFunSuite

class OrderComputerTest extends SparkFunSuite {

  test("orderTest") {
    val numRelation = 6
    val arity = 4
    val cardinality = 10000
    val query = QueryGenerator.genRandomQuery(numRelation, arity, cardinality)

    val relations = query._1.zip(query._2).map {
      case (content, schema) =>
        val rdd = sc.parallelize(content)
        Relation(schema, rdd)
    }

    val statistic = Statistic.defaultStatistic()
    relations.foreach(relation => statistic.add(relation))

    val orderComputer = new OrderComputer(query._2)

    println(s"query:${query._2}")
    println(
      s"all order is ${orderComputer.genAllOrderWithCost().map(f => (f._1.toSeq, f._2)).toArray.toSeq}"
    )
    println(s"selected order is ${orderComputer.optimalOrder().toSeq}")
  }

}
