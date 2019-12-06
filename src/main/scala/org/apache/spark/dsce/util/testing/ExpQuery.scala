package org.apache.spark.dsce.util.testing

import org.apache.spark.adj.database.{Catalog, Relation}
import org.apache.spark.adj.execution.misc.DataLoader
import org.apache.spark.adj.utils.exp.ExpQueryHelper

class ExpQuery(data: String) {

  val rdd = new DataLoader().csv(data)
  val defaultCore = "A"

  def getDml(q: String) = {
    val dml = q match {
      //2-node
      case "edge" => edgeDml

      //3-node
      case "wedge"    => wedgeDml
      case "triangle" => triangleDml

      //4-node
      case "threePath"     => threePathDml
      case "threeStar"     => threeStarDml
      case "square"        => squareDml
      case "triangleEdge"  => triangleEdgeDml
      case "chordalSquare" => chordalSquareDml
      case "fourClique"    => fourCliqueDml

      //5-node
      case "house"         => houseDml
      case "threeTriangle" => threeTriangleDml
      case "solarSquare"   => solarSquareDml
      case "near5Clique"   => near5CliqueSchemaDml

      case "fiveClique"         => fiveCliqueDml
      case "l32"                => l32Dml
      case "fiveCliqueMinusOne" => fiveCliqueMinusOneDml
      case "squareEdge"         => squareEdgeDml
      case "chordalSquare"      => chordalSquareDml
      case "chordalSquareEdge"  => chordalSquareEdgeDml
      case "fourCliqueEdge"     => fourCliqueEdgeDml

      //6-node
      case "quadTriangle"    => quadTriangleDml
      case "triangleCore"    => triangleCoreDml
      case "twinCSquare"     => twinCSquareDml
      case "twinClique4"     => twinClique4Dml
      case "starofDavidPlus" => starofDavidPlusDml

      case "b313" => b313Dml
      case _      => throw new Exception(s"no such pattern:${q}")
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

  //------------------2-node------------------
  //edge
  private val edgeDml = "A-B;"

  //------------------3-node------------------
  //wedge
  private val wedgeDml = "A-B;A-C;"

  //triangle
  private val triangleDml = "A-B;B-C;C-A;"

  //------------------4-node------------------

  //threePath
  private val threePathDml = "A-B;B-C;C-D;"

  //threeStar
  private val threeStarDml = "A-B;A-C;A-D;"

  //square
  private val squareDml = "A-B;B-C;C-D;D-A;"

  //triangleEdge
  private val triangleEdgeDml = "A-B;B-D;D-A;C-D;"

  //  chordalSquare
  private val chordalSquareDml = "A-B;B-D;D-A;B-C;C-D;"

  //fourClique
  private val fourCliqueDml = "A-B;B-C;C-D;D-A;A-C;B-D;"

  //------------------5-node------------------

  //  house
  private val houseDml = "A-B;B-C;C-D;D-E;E-A;B-E;"

  //  threeTriangle
  private val threeTriangleDml = "A-B;B-C;C-D;D-E;E-A;B-E;C-E;"

  //  solarSquare
  private val solarSquareDml = "A-B;B-C;C-D;D-E;E-A;A-D;B-E;C-E;"

  //  near5Clique
  private val near5CliqueSchemaDml = "A-B;B-C;C-D;D-E;E-A;B-E;B-D;C-E;"

  //  fiveClique
  private val fiveCliqueDml = "A-B;B-C;C-D;D-E;E-A;A-C;A-D;B-D;B-E;C-E;"

  //  fiveCliqueMinusOne, A-D removed
  private val fiveCliqueMinusOneDml = "A-B;B-C;C-D;D-E;E-A;A-C;B-D;B-E;C-E;"

  //  lolipop
  private val l32Dml = "A-B;B-C;C-A;A-D;D-E;"

  //squareEdge
  private val squareEdgeDml = "A-B;B-C;C-D;D-A;C-E;"

  //chordalSquareEdge
  private val chordalSquareEdgeDml = "A-B;B-C;C-D;D-A;B-D;A-E;"

  //fourCliqueEdge
  private val fourCliqueEdgeDml = "A-B;B-C;C-D;D-A;A-C;B-D;A-E;"

  //------------------6-node------------------

  //quadTriangle
  private val quadTriangleDml = "A-B;B-C;C-D;D-E;E-F;F-A;B-D;B-E;B-F;"

  //triangleCore
  private val triangleCoreDml = "A-B;B-C;C-D;D-E;E-F;F-A;B-D;B-F;D-F;"

  //twinCSquare
  private val twinCSquareDml = "A-B;B-C;C-D;D-E;E-F;F-A;A-C;A-D;D-F;"

  //twinClique4
  private val twinClique4Dml = "A-B;B-C;C-D;D-E;E-F;F-A;A-C;B-F;C-E;C-F;D-F;"

  //starofDavidPlus
  private val starofDavidPlusDml =
    "A-B;B-C;C-D;D-E;E-F;F-A;A-C;A-E;B-D;B-F;C-E;C-F;D-F;"

  //  barbell
  private val b313Dml = "A-B;B-C;C-A;C-D;D-E;D-F;E-F;"

}
