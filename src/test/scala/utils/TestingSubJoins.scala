package utils

import hzhang.test.exp.utils.DataGenerator
import org.apache.spark.adj.database.Database.AttributeID
import org.apache.spark.adj.database.{Relation, RelationSchema}
import org.apache.spark.adj.hcube.{HCubePlan, TupleHCubeBlock}
import org.apache.spark.adj.plan.{AttributeOrderInfo, SubJoin}
import org.apache.spark.adj.utlis.SparkSingle

import scala.io.Source
import scala.util.Random


object TestingSubJoins {

  lazy val sc = SparkSingle.getSparkContext()

  lazy val testing_query2_schema = {
    val R0_Schema = RelationSchema("R0", Seq("A", "B"))
    val R1_Schema = RelationSchema("R1", Seq("B", "C"))
    val R2_Schema = RelationSchema("R2", Seq("A", "C"))

    val relationSchemas = Seq(R0_Schema, R1_Schema, R2_Schema)
    relationSchemas.foreach(_.register())
    relationSchemas
  }

  lazy val testing_query1_schema = {
    val R0_Schema = RelationSchema("R0", Seq("A", "B", "C"))
    val R1_Schema = RelationSchema("R1", Seq("C", "D"))
    val R2_Schema = RelationSchema("R2", Seq("E", "C", "B"))

    val relationSchemas = Seq(R0_Schema, R1_Schema, R2_Schema)
    relationSchemas.foreach(_.register())
    relationSchemas
  }

  lazy val testing_subjoins1 = {

    val cardinality = 1000
    val query_schema = testing_query1_schema
    val contents =  query_schema.map(r => ContentGenerator.genIdentityContent(cardinality, r.attrs.size))
    val blocks = contents.zip(query_schema).map{
      case (content, r) =>
        TupleHCubeBlock(r, null, content)
    }

    val attrOrder = query_schema.flatMap(_.attrIDs).distinct.toArray
    val notImportantShare = Array(1, 2, 3, 4, 5)

    new SubJoin(notImportantShare, blocks, AttributeOrderInfo(attrOrder))
  }

  lazy val testing_subjoins2 = {

    val cardinality = 10
//    genIdentityContent(cardinality, r.attrs.size)
//    genGraphContent()
    val query_schema = testing_query2_schema
    val dataName = "wikiV"
    val contents =  query_schema.map(r => ContentGenerator.genGraphContent(dataName))
    val blocks = contents.zip(query_schema).map{
      case (content, r) =>
        TupleHCubeBlock(r, null, content)
    }

    val attrOrder = query_schema.flatMap(_.attrIDs).distinct.toArray
    val notImportantShare = Array(1, 2, 3, 4, 5)


    new SubJoin(notImportantShare, blocks, AttributeOrderInfo(attrOrder))
  }


  lazy val testing_query1_hcubePlan = {
    val cardinality = 100
    val query_schema = testing_query1_schema
    val share = Map(
      (0, 1),
      (1, 2),
      (2, 3),
      (3, 4),
      (4, 5)
    )

    val R0_content =  sc.parallelize(ContentGenerator.genRandomContent(cardinality, query_schema(0).attrIDs.size))
    val R1_content =  sc.parallelize(ContentGenerator.genRandomContent(cardinality, query_schema(1).attrIDs.size))
    val R2_content =  sc.parallelize(ContentGenerator.genRandomContent(cardinality, query_schema(2).attrIDs.size))

    val R0 = Relation(query_schema(0), R0_content)
    val R1 = Relation(query_schema(1), R1_content)
    val R2 = Relation(query_schema(2), R2_content)

    val relations = Array(R0, R1, R2)

    HCubePlan(relations, share)
  }


}

