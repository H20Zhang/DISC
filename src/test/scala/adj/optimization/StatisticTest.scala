package adj.optimization

import adj.SparkFunSuite
import org.apache.spark.adj.optimization.stat.{Statistic, StatisticComputer}
import org.apache.spark.adj.utils.misc.SparkSingle
import org.apache.spark.adj.utils.testing.RelationGenerator
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class StatisticTest extends SparkFunSuite {

  test("statistic computer") {

    val relation = RelationGenerator.genRandomRelation(4, 10000)
    val statistic = Statistic.defaultStatistic()
    statistic.add(relation)
    val schema = relation.schema

    println(s"cardinality:${statistic.cardinality(schema)}")

    val attr0_2 = schema.attrIDs.slice(0, 2)
    println(
      s"cardinality of attribute ${attr0_2}:${statistic.cardinality(schema, attr0_2)}"
    )

    val attr0_3 = schema.attrIDs.slice(0, 3)
    println(
      s"cardinality of attribute ${attr0_3}:${statistic.cardinality(schema, attr0_3)}"
    )

    println(s"relative degree ${attr0_3} - ${attr0_2}:${statistic
      .relativeDegree(schema, attr0_3, attr0_2)}")

    val attr0_0 = schema.attrIDs.slice(0, 0)
    println(s"relative degree ${attr0_3} - ${attr0_0}:${statistic
      .relativeDegree(schema, attr0_3, attr0_0)}")
  }
}
