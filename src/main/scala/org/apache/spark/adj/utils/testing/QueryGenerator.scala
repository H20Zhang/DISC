package org.apache.spark.adj.utils.testing

import org.apache.spark.adj.database.Catalog.DataType
import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.execution.hcube.TupleHCubeBlock
import org.apache.spark.adj.utils.extension.SeqUtil

import scala.util.Random

object QueryGenerator {

  def genDebugSubJoin() = {

    val domainSize = 10
    val attributes = Seq("A", "B", "C")
    val schema1 = RelationSchema("R0", Seq("A", "B", "C"))
    val schema2 = RelationSchema("R1", Seq("B"))
    val schema3 = RelationSchema("R2", Seq("A", "C"))
    val schema4 = RelationSchema("R3", Seq("A", "B"))

    val schemas = Seq(schema1, schema2, schema3, schema4)
    schemas.foreach { schema =>
      schema.register()
    }

    val content1 = Array(
      Array(1, 4, 0),
      Array(0, 3, 3),
      Array(4, 3, 1),
      Array(2, 3, 0),
      Array(5, 1, 5),
      Array(5, 5, 5),
      Array(5, 3, 4),
      Array(5, 3, 1),
      Array(1, 0, 0),
      Array(0, 5, 1),
      Array(0, 0, 0),
      Array(4, 3, 5),
      Array(3, 5, 2),
      Array(0, 2, 0),
      Array(3, 0, 5),
      Array(2, 2, 4),
      Array(4, 5, 0),
      Array(3, 2, 5),
      Array(3, 2, 3),
      Array(2, 5, 3),
      Array(5, 1, 2),
      Array(5, 1, 0),
      Array(3, 3, 4),
      Array(2, 2, 0),
      Array(3, 1, 5),
      Array(2, 1, 4),
      Array(2, 5, 1),
      Array(1, 4, 5),
      Array(2, 0, 2),
      Array(1, 1, 4),
      Array(4, 2, 4),
      Array(0, 1, 3),
      Array(0, 1, 1),
      Array(3, 5, 5),
      Array(0, 2, 5),
      Array(1, 4, 2),
      Array(2, 3, 2),
      Array(5, 1, 3),
      Array(2, 5, 4),
      Array(0, 0, 1),
      Array(1, 5, 3),
      Array(1, 4, 3),
      Array(4, 0, 1),
      Array(1, 3, 0),
      Array(4, 1, 5),
      Array(2, 5, 0),
      Array(2, 1, 1),
      Array(0, 3, 4),
      Array(1, 0, 2),
      Array(3, 4, 0),
      Array(4, 1, 1),
      Array(4, 2, 5),
      Array(2, 3, 4),
      Array(5, 0, 2),
      Array(0, 2, 1),
      Array(4, 1, 0),
      Array(2, 4, 1),
      Array(2, 0, 4),
      Array(1, 5, 4),
      Array(3, 1, 1),
      Array(5, 0, 0),
      Array(5, 0, 3),
      Array(2, 3, 1),
      Array(3, 1, 4),
      Array(5, 4, 5),
      Array(0, 0, 3),
      Array(3, 5, 3),
      Array(5, 4, 1),
      Array(0, 2, 2),
      Array(2, 1, 5),
      Array(0, 3, 2),
      Array(0, 4, 2),
      Array(1, 4, 4),
      Array(1, 0, 5),
      Array(0, 4, 4),
      Array(1, 4, 1),
      Array(0, 0, 2),
      Array(3, 1, 0),
      Array(2, 1, 3),
      Array(5, 4, 4),
      Array(4, 4, 1),
      Array(0, 1, 0),
      Array(0, 5, 4),
      Array(4, 2, 0),
      Array(3, 4, 1),
      Array(0, 0, 5),
      Array(4, 0, 4),
      Array(1, 5, 2),
      Array(4, 5, 2),
      Array(2, 0, 5),
      Array(5, 1, 4),
      Array(1, 5, 5),
      Array(0, 2, 3),
      Array(5, 1, 1),
      Array(3, 4, 4),
      Array(4, 1, 3),
      Array(3, 5, 0),
      Array(5, 5, 4),
      Array(4, 2, 1),
      Array(3, 3, 1),
      Array(2, 0, 1),
      Array(5, 5, 3),
      Array(2, 2, 3),
      Array(1, 1, 5),
      Array(3, 5, 1),
      Array(5, 2, 2),
      Array(5, 4, 3),
      Array(0, 3, 0),
      Array(3, 4, 2),
      Array(5, 3, 5),
      Array(0, 5, 0)
    )

    val content2 = Array(Array(1), Array(2), Array(3), Array(5))

    val content3 = Array(
      Array(4, 1),
      Array(5, 0),
      Array(5, 2),
      Array(2, 2),
      Array(2, 1),
      Array(0, 4),
      Array(3, 2),
      Array(3, 0),
      Array(3, 4),
      Array(0, 2),
      Array(0, 3),
      Array(1, 5),
      Array(1, 3),
      Array(5, 4)
    )

    val content4 = Array(
      Array(5, 2),
      Array(4, 1),
      Array(3, 3),
      Array(1, 2),
      Array(2, 5),
      Array(5, 1),
      Array(1, 1),
      Array(4, 3),
      Array(2, 2),
      Array(2, 1),
      Array(0, 1),
      Array(1, 5),
      Array(5, 0),
      Array(2, 0),
      Array(0, 2),
      Array(3, 4),
      Array(2, 4),
      Array(4, 4),
      Array(3, 2),
      Array(0, 4),
      Array(4, 2)
    )

    val contents = Seq(content1, content2, content3, content4)

    (contents, schemas)
  }

  //generate a random query and according relations, output is in the form of simple DML
  def genRandomQuery(
    numRelation: Int,
    arity: Int,
    cardinality: Int
  ): (Seq[Array[Array[DataType]]], Seq[RelationSchema]) = {
    val domainSize =
      Math.pow(cardinality.toDouble, 1 / arity.toDouble).ceil.toInt
    val attributes = Range(0, arity).map(_.toString)
    val subsets = Random.shuffle(SeqUtil.subset(attributes))

    val schemas = Range(0, numRelation).map { i =>
      val schema = RelationSchema(s"R${i}", subsets(i))
      schema.register()
      schema
    }

    val contents = schemas.map { schema =>
      ContentGenerator
        .genCatersianProductContent(domainSize, schema.attrIDs.size)
        .distinct
    }

    (contents, schemas)
  }

  //generate a random query and according relations, output is in the form of simple DML
  def genRandomUnaryRelationQuery(
    numRelation: Int,
    cardinality: Int
  ): (Seq[Array[DataType]], Seq[RelationSchema]) = {

    val attributes = Range(0, numRelation).map(_.toString)

    val schemas = Range(0, numRelation).map { i =>
      val schema = RelationSchema(s"R${i}", Seq(attributes(i)))
      schema.register()
      schema
    }

    val contents = schemas.map { schema =>
      ContentGenerator.genRandomUnaryContent(cardinality).distinct
    }

    (contents, schemas)
  }

//  //generate a random graph query and according relations, output is in the form of simple DML
//  def genRandomGraphSubJoin(vertexSize:Int, edgePercentage:Double):String = ???

}
