package org.apache.spark.dsce.testing

import org.apache.spark.adj.database.{Catalog, Relation}
import org.apache.spark.adj.execution.misc.DataLoader
import org.apache.spark.adj.utils.exp.ExpQueryHelper
import org.apache.spark.dsce.DISCConf

class ExpQuery(data: String) {

  lazy val rdd = new DataLoader().csv(data)
  val core = DISCConf.defaultConf().core

  def getDml(q: String) = {
    val dml = q match {

      case "debug" => debugDml
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

      case "fiveCycle"          => fiveCycleDml
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

      //4-profile queries
      case "g1"  => g1
      case "g2"  => g2
      case "g3"  => g3
      case "g4"  => g4
      case "g5"  => g5
      case "g6"  => g6
      case "g7"  => g7
      case "g8"  => g8
      case "g9"  => g9
      case "g10" => g10
      case "g11" => g11

      //5-eclog
      case "g13" => g13
      case "g14" => g14
      case "g15" => g15
      case "g16" => g16
      case "g17" => g17
      case "g18" => g18

      case "g20" => g20
      case "g21" => g21
      case "g22" => g22
      case "g23" => g23
      case "g24" => g24
      case "g25" => g25
      case "g26" => g26
      case "g27" => g27
      case "g28" => g28
      case "g29" => g29
      case "g30" => g30
      case "g31" => g31
      case "g32" => g32
      case "g33" => g33
      case "g34" => g34
      case "g35" => g35
      case "g36" => g36
      case "g37" => g37
      case "g38" => g38
      case "g39" => g39
      case "g40" => g40
      case "g41" => g41
      case "g42" => g42
      case "g43" => g43
      case "g44" => g44
      case "g45" => g45

//        five profile queries
      case "t1"  => t1
      case "t2"  => t2
      case "t3"  => t3
      case "t4"  => t4
      case "t5"  => t5
      case "t6"  => t6
      case "t7"  => t7
      case "t8"  => t8
      case "t9"  => t9
      case "t10" => t10
      case "t11" => t11
      case "t12" => t12
      case "t13" => t13
      case "t14" => t14
      case "t15" => t15
      case "t16" => t16
      case "t17" => t17
      case "t18" => t18
      case "t19" => t19
      case "t20" => t20
      case "t21" => t21
      case "t22" => t22
      case "t23" => t23
      case "t24" => t24
      case "t25" => t25
      case "t26" => t26
      case "t27" => t27
      case "t28" => t28
      case "t29" => t29
      case "t30" => t30
      case "t31" => t31
      case "t32" => t32
      case "t33" => t33
      case "t34" => t34
      case "t35" => t35
      case "t36" => t36
      case "t37" => t37
      case "t38" => t38
      case "t39" => t39
      case "t40" => t40
      case "t41" => t41
      case "t42" => t42
      case "t43" => t43
      case "t44" => t44
      case "t45" => t45
      case "t46" => t46
      case "t47" => t47
      case "t48" => t48
      case "t49" => t49
      case "t50" => t50
      case "t51" => t51
      case "t52" => t52
      case "t53" => t53
      case "t54" => t54
      case "t55" => t55
      case "t56" => t56
      case "t57" => t57
      case "t58" => t58

      //debug
      case "Dwedge1" => Dwedge1
      case "Dwedge2" => Dwedge2

      case "DthreePath1" => DthreePath1
      case "DthreePath2" => DthreePath2

      case "DtriangleEdge1" => DtriangleEdge1
      case "DtriangleEdge2" => DtriangleEdge2
      case "DtriangleEdge3" => DtriangleEdge3

      case "DchordalSquare1" => DchordalSquare1
      case "DchordalSquare2" => DchordalSquare2

      case _ => throw new Exception(s"no such pattern:${q}")
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
      s"SubgraphCount ${(schemas ++ notIncludedEdgesSchemas)
        .map(schema => s"${schema.name};")
        .reduce(_ + _)
        .dropRight(1)} on $core;"
    query0
  }

  //experiment query
  private val debugDml = "A-B;E-F;F-A;B-F;B-E;"

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

  //  fiveCycle
  private val fiveCycleDml = "A-B;B-C;C-D;D-E;E-A;"

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

  //4-profile queries
  private val g1 = "A-B;B-C;A-D;"
  private val g2 = "A-B;B-C;B-D;"
  private val g3 = "A-B;B-C;C-D;A-D;"
  private val g4 = "A-B;B-C;C-D;B-D;"
  private val g5 = "A-B;A-C;B-C;C-D;B-D;"
  private val g6 = "A-B;A-C;A-D;B-C;B-D;C-D;"
  private val g7 = "A-B;A-C;A-D;"
  private val g8 = "A-B;B-C;C-D;"
  private val g9 = "A-B;B-C;A-C;A-D;"
  private val g10 = "A-B;B-C;A-C;A-D;C-D;"
  private val g11 = "A-B;A-C;B-C;B-D;"

  //5-edge centric queries
  private val g13 = "A-B;B-C;B-D;A-E;"
  private val g14 = "A-B;B-C;B-E;B-D;"
  private val g15 = "A-B;A-C;A-D;B-C;B-E;"
  private val g16 = "A-B;A-C;A-D;C-D;B-E;"
  private val g17 = "A-B;A-C;B-C;B-D;B-E;"
  private val g18 = "A-B;A-D;A-E;A-C;D-E;"

  private val g20 = "A-B;B-C;C-D;D-A;B-E;"
  private val g21 = "A-B;A-C;A-D;B-C;B-D;B-E;"
  private val g22 = "A-B;A-E;B-E;B-D;D-E;B-C;"
  private val g23 = "A-B;A-C;A-D;A-E;C-D;D-E;"
  private val g24 = "A-B;A-C;B-C;B-D;B-E;D-E;"
  private val g25 = "A-B;B-D;A-D;D-E;A-E;B-C;"
  private val g26 = "A-B;A-C;C-D;B-D;A-E;D-E;"
  private val g27 = "A-B;A-C;B-C;A-D;D-E;B-E;"
  private val g28 = "A-B;A-D;A-E;B-C;C-D;D-E;"
  private val g29 = "A-B;A-C;B-C;B-D;B-E;C-D;C-E;"
  private val g30 = "A-B;A-C;A-D;A-E;B-C;B-D;B-E;"
  private val g31 = "A-B;A-C;A-D;A-E;B-C;B-D;C-D;"
  private val g32 = "A-B;A-C;A-D;A-E;C-D;C-E;D-E;"
  private val g33 = "A-B;A-C;B-C;B-D;B-E;C-D;D-E;"
  private val g34 = "A-B;A-C;A-D;B-C;B-D;B-E;D-E;"
  private val g35 = "A-B;A-C;A-D;B-D;B-E;C-D;D-E;"
  private val g36 = "A-B;A-C;A-E;B-C;B-D;C-D;D-E;"
  private val g37 = "A-B;A-D;A-E;B-C;C-D;C-E;D-E;"
  private val g38 = "A-B;A-C;B-C;B-D;B-E;C-D;C-E;D-E;"
  private val g39 = "A-B;A-C;A-D;A-E;B-C;B-D;B-E;D-E;"
  private val g40 = "A-B;A-C;A-D;A-E;B-D;B-E;C-D;D-E;"
  private val g41 = "A-B;A-C;A-D;B-C;B-E;C-D;C-E;D-E;"
  private val g42 = "A-B;A-C;A-E;B-C;B-D;B-E;C-D;D-E;"
  private val g43 = "A-B;A-C;A-D;B-C;B-D;B-E;C-D;C-E;D-E;"
  private val g44 = "A-B;A-C;A-D;A-E;B-C;B-D;B-E;C-D;D-E;"
  private val g45 = "A-B;A-C;A-D;A-E;B-C;B-D;B-E;C-D;C-E;D-E;"

  //five profile query
  private val t1 = "A-B;A-C;A-D;A-E;"
  private val t2 = "A-B;B-C;B-D;B-E;"
  private val t3 = "A-B;A-C;A-D;B-E;"
  private val t4 = "A-B;A-E;B-C;B-D;"
  private val t5 = "A-B;B-C;B-D;C-E;"
  private val t6 = "A-C;B-C;B-D;B-E;"
  private val t7 = "A-B;A-C;B-D;C-E;"
  private val t8 = "A-B;A-D;B-C;C-E;"
  private val t9 = "A-C;B-C;B-D;D-E;"
  private val t10 = "A-B;A-C;A-D;A-E;B-C;"
  private val t11 = "A-B;A-C;B-C;B-D;B-E;"
  private val t12 = "A-B;B-C;B-D;B-E;C-D;"
  private val t13 = "A-B;A-C;A-D;B-C;B-E;"
  private val t14 = "A-B;A-C;B-C;B-D;C-E;"
  private val t15 = "A-B;B-C;B-D;C-D;C-E;"
  private val t16 = "A-B;A-C;A-D;B-C;D-E;"
  private val t17 = "A-B;A-C;B-C;B-D;D-E;"
  private val t18 = "A-B;A-E;B-C;B-D;C-D;"
  private val t19 = "A-E;B-C;B-D;B-E;C-D;"
  private val t20 = "A-B;A-C;A-D;B-E;C-E;"
  private val t21 = "A-B;A-E;B-C;B-D;C-E;"
  private val t22 = "A-B;B-C;B-D;C-E;D-E;"
  private val t23 = "A-C;A-D;B-C;B-D;B-E;"
  private val t24 = "A-B;A-C;B-D;C-E;D-E;"
  private val t25 = "A-B;A-C;A-D;A-E;B-C;B-D;"
  private val t26 = "A-B;A-C;A-D;B-C;B-D;B-E;"
  private val t27 = "A-B;A-C;B-C;B-D;B-E;C-D;"
  private val t28 = "A-B;B-C;B-D;B-E;C-D;C-E;"
  private val t29 = "A-B;A-C;A-D;A-E;B-C;D-E;"
  private val t30 = "A-B;A-C;B-C;B-D;B-E;D-E;"
  private val t31 = "A-B;A-C;A-D;B-C;B-D;C-E;"
  private val t32 = "A-B;A-C;A-E;B-C;B-D;C-D;"
  private val t33 = "A-B;A-C;B-C;B-D;C-D;D-E;"
  private val t34 = "A-D;B-C;B-D;B-E;C-D;C-E;"
  private val t35 = "A-B;A-C;A-D;B-C;B-E;D-E;"
  private val t36 = "A-B;A-C;B-C;B-D;C-E;D-E;"
  private val t37 = "A-B;A-E;B-C;B-D;C-D;C-E;"
  private val t38 = "A-B;A-C;A-D;B-E;C-E;D-E;"
  private val t39 = "A-B;A-E;B-C;B-D;C-E;D-E;"
  private val t40 = "A-B;A-C;A-D;A-E;B-C;B-D;B-E;"
  private val t41 = "A-B;A-C;B-C;B-D;B-E;C-D;C-E;"
  private val t42 = "A-B;A-C;A-D;A-E;B-C;B-D;C-D;"
  private val t43 = "A-B;A-C;A-D;B-C;B-D;B-E;C-D;"
  private val t44 = "A-B;B-C;B-D;B-E;C-D;C-E;D-E;"
  private val t45 = "A-B;A-C;A-D;A-E;B-C;B-D;C-E;"
  private val t46 = "A-B;A-C;A-D;B-C;B-D;B-E;C-E;"
  private val t47 = "A-B;A-C;B-C;B-D;B-E;C-D;D-E;"
  private val t48 = "A-B;A-C;A-D;B-C;B-D;C-E;D-E;"
  private val t49 = "A-B;A-C;A-E;B-C;B-D;C-D;D-E;"
  private val t50 = "A-D;A-E;B-C;B-D;B-E;C-D;C-E;"
  private val t51 = "A-B;A-C;A-D;A-E;B-C;B-D;B-E;C-D;"
  private val t52 = "A-B;A-C;A-D;B-C;B-D;B-E;C-D;C-E;"
  private val t53 = "A-B;A-C;B-C;B-D;B-E;C-D;C-E;D-E;"
  private val t54 = "A-B;A-C;A-D;A-E;B-C;B-D;C-E;D-E;"
  private val t55 = "A-B;A-C;A-D;B-C;B-D;B-E;C-E;D-E;"
  private val t56 = "A-B;A-C;A-D;A-E;B-C;B-D;B-E;C-D;C-E;"
  private val t57 = "A-B;A-C;A-D;B-C;B-D;B-E;C-D;C-E;D-E;"
  private val t58 = "A-B;A-C;A-D;A-E;B-C;B-D;B-E;C-D;C-E;D-E;"

  //debug
  private val Dwedge1 = "A-B;B-C;"
  private val Dwedge2 = "A-B;A-C;"

  private val DthreePath1 = "A-B;B-C;C-D;"
  private val DthreePath2 = "A-B;A-C;C-D;"

  private val DtriangleEdge1 = "A-B;B-C;B-D;C-D;"
  private val DtriangleEdge2 = "A-B;A-C;A-D;C-D;"
  private val DtriangleEdge3 = "A-B;A-C;B-C;C-D;"

  private val DchordalSquare1 = "A-B;A-C;A-D;B-C;C-D;"
  private val DchordalSquare2 = "A-B;A-D;B-C;B-D;C-D;"
}
