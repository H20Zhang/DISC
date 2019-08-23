package utils

import org.apache.spark.adj.database.Database.AttributeID
import org.apache.spark.adj.database.{Relation, RelationSchema}
import org.apache.spark.adj.hcube.TupleHCubeBlock
import org.apache.spark.adj.plan.{HCubePlan, SubJoin}
import org.apache.spark.adj.utlis.SparkSingle

import scala.util.Random


object TestingData {

  lazy val sc = SparkSingle.getSparkContext()


  def genTestingContent(cardinality:Int, artiy:Int) = {
    val table = Range(0,cardinality).map{
      _ =>
        Range(0, artiy).map{
          _ => Math.abs(Random.nextInt() % (2*cardinality))
        }.toSeq
    }.distinct.toArray.map(_.toArray)

    table
  }

  lazy val testing_query1_schema = {
    val R0_Schema = RelationSchema("R0", Seq("A", "B", "C"))
    val R1_Schema = RelationSchema("R1", Seq("C", "D"))
    val R2_Schema = RelationSchema("R2", Seq("E", "C", "B"))

    val relationSchemas = Seq(R0_Schema, R1_Schema, R2_Schema)
    relationSchemas.foreach(_.register())
    relationSchemas
  }

  lazy val testing_query1_hcubePlan = {
    val cardinality = 2
    val query_schema = testing_query1_schema
    val share = Map(
      (1, 2),
      (2, 3),
      (3, 4),
      (4, 5),
      (5, 2)
    )

    val R0_content =  sc.parallelize(genTestingContent(cardinality, query_schema(0).attrIDs.size))
    val R1_content =  sc.parallelize(genTestingContent(cardinality, query_schema(1).attrIDs.size))
    val R2_content =  sc.parallelize(genTestingContent(cardinality, query_schema(2).attrIDs.size))

    val R0 = Relation(query_schema(0), R0_content)
    val R1 = Relation(query_schema(0), R1_content)
    val R2 = Relation(query_schema(0), R2_content)

    val relations = Array(R0, R1, R2)

    new HCubePlan(relations, share)
  }

  lazy val testing_subjoins1 = {

    val cardinality = 10
    val query_schema = testing_query1_schema
    val contents =  query_schema.map(r => genTestingContent(cardinality, r.attrs.size))
    val blocks = contents.zip(query_schema).map{
      case (content, r) =>
        TupleHCubeBlock(r, null, content)
    }

    val attrOrder = query_schema.flatMap(_.attrIDs).distinct.toArray

    new SubJoin(blocks, attrOrder)
  }
}

class TestingRelationContentGenerator {



}

object TestingQuery {

}

