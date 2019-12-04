package org.apache.spark.dsce.util.testing

import org.apache.spark.adj.database.{Catalog, Relation}
import org.apache.spark.adj.execution.misc.DataLoader
import org.apache.spark.adj.utils.exp.ExpQueryHelper

class ExpQuery(data: String) {

  val rdd = new DataLoader().csv(data)
  val defaultCore = "A"

  def getDml(q: String) = {
    val dml = q match {
      case "edge"               => edgeDml
      case "wedge"              => wedgeDml
      case "triangle"           => triangleDml
      case "chordalSquare"      => chordalSquareDml
      case "fourClique"         => fourCliqueDml
      case "fiveClique"         => fiveCliqueDml
      case "l31"                => l31Dml
      case "l32"                => l32Dml
      case "b313"               => b313Dml
      case "house"              => houseDml
      case "threeTriangle"      => threeTriangleDml
      case "solarSquare"        => solarSquareDml
      case "near5Clique"        => near5CliqueSchemaDml
      case "fiveCliqueMinusOne" => fiveCliqueMinusOneDml
      case "triangleEdge"       => triangleEdgeDml
      case "square"             => squareDml
      case "squareEdge"         => squareEdgeDml
      case "chordalSquare"      => chordalSquareDml
      case "threePath"          => threePathDml
      case "chordalSquareEdge"  => chordalSquareEdgeDml
      case "fourCliqueEdge"     => fourCliqueEdgeDml
      case _                    => throw new Exception(s"no such pattern:${q}")
    }

    assert(dml.endsWith(";"))

    dml
  }

  def getSchema(q: String) = {
    val dml = getDml(q)
    ExpQueryHelper.dmlToSchemas(dml)
  }

  def getNotIncludedEdgesSchema(q: String) = {
    val dml = getDml(q)
    ExpQueryHelper.dmlToNotIncludedEdgeSchemas(dml)
  }

  def getRelations(q: String) = {
    val schemas = getSchema(q)
    schemas.foreach(schema => Catalog.defaultCatalog().setContent(schema, rdd))
    schemas.map(schema => Relation(schema, rdd))
  }

  def getQuery(q: String) = {
    val schemas = getSchema(q)
    schemas.foreach(schema => Catalog.defaultCatalog().setContent(schema, rdd))

    val notIncludedEdgesSchemas = getNotIncludedEdgesSchema(q)
    notIncludedEdgesSchemas.foreach(
      schema => Catalog.defaultCatalog().setContent(schema, rdd)
    )

    val query0 =
      s"SubgraphCount ${(schemas ++ notIncludedEdgesSchemas).map(schema => s"${schema.name};").reduce(_ + _).dropRight(1)} on A;"
    query0
  }

  //experiment query

  //edge
  private val edgeDml = "A-B;"

  //wedge
  private val wedgeDml = "A-B;A-C;"

  //triangle
  private val triangleDml = "A-B;B-C;C-A;"

  //fourClique
  private val fourCliqueDml = "A-B;B-C;C-D;D-A;A-C;B-D;"

  //  fiveClique
  private val fiveCliqueDml = "A-B;B-C;C-D;D-E;E-A;A-C;A-D;B-D;B-E;C-E;"

  //  house
  private val houseDml = "A-B;B-C;C-D;D-E;E-A;B-E;"

  //  threeTriangle
  private val threeTriangleDml = "A-B;B-C;C-D;D-E;E-A;B-E;C-E;"

  //  near5Clique
  private val near5CliqueSchemaDml = "A-B;B-C;C-D;D-E;E-A;B-E;B-D;C-E;"

  //  fiveCliqueMinusOne, A-D removed
  private val fiveCliqueMinusOneDml = "A-B;B-C;C-D;D-E;E-A;A-C;B-D;B-E;C-E;"

  //optional query
  //  lolipop
  private val l31Dml = "A-B;B-C;C-A;A-D;"
  private val l32Dml = "A-B;B-C;C-A;A-D;D-E;"

  //  barbell
  private val b313Dml = "A-B;B-C;C-A;C-D;D-E;D-F;E-F;"

  //  solarSquare
  private val solarSquareDml = "A-B;B-C;C-D;D-A;A-E;B-E;C-E;D-E;"

  //triangleEdge
  private val triangleEdgeDml = "A-B;B-C;C-A;C-D;"

  //square
  private val squareDml = "A-B;B-C;C-D;D-A;"

  //squareEdge
  private val squareEdgeDml = "A-B;B-C;C-D;D-A;C-E;"

  //  chordalSquare
  private val chordalSquareDml = "A-B;B-C;C-D;D-A;B-D;"

  //threePath
  private val threePathDml = "A-B;B-C;C-D;"

  //chordalSquareEdge
  private val chordalSquareEdgeDml = "A-B;B-C;C-D;D-A;B-D;A-E;"

  //fourCliqueEdge
  private val fourCliqueEdgeDml = "A-B;B-C;C-D;D-A;A-C;B-D;A-E;"

}
