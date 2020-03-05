package org.apache.spark.dsce.optimization.aggregate

import org.apache.spark.adj.database.{Catalog, RelationSchema}
import org.apache.spark.adj.optimization.costBased.decomposition.relationGraph.RelationGHDTree
import org.apache.spark.dsce.util.Graph

class ManualRelationDecomposer(schemas: Seq[RelationSchema]) {

  def decomposeTree(): IndexedSeq[RelationGHDTree] = {

    val quadTriangleRule = (quadTriangleGraph, quadTriangleGHD)
    val triangleCoreRule = (triangleCoreGraph, triangleCoreGHD)
    val twinCSquareRule = (twinCSquareGraph, twinCSquareGHD)
    val twinClique4Rule = (twinClique4Graph, twinClique4GHD)

    val rules =
      Seq(quadTriangleRule, triangleCoreRule, twinCSquareRule, twinClique4Rule)

    val inputGraph = schemasToGraph()

//    println(s"rules:$rules")
    val availableGHDGraph =
      rules.map(f => (stringToGraph(f._1), f._2))

    availableGHDGraph.foreach {
      case (ghdGraph, ghdString) =>
        if (ghdGraph.isIsomorphic(inputGraph)) {
          return IndexedSeq(stringToGHD(ghdString))
        }
    }

    IndexedSeq()
  }

  def schemasToGraph(): Graph = {
    val E = schemas.map { f =>
      val attrIds = f.attrIDs
      val edge = (attrIds(0), attrIds(1))
      edge
    }

    val V = E.flatMap(f => Seq(f._1, f._2)).distinct

    new Graph(V, E)
  }

  def stringToGHD(str: String): RelationGHDTree = {

    val catalog = Catalog.defaultCatalog()
    val edgeToSchemaMap = schemas.map { f =>
      val attrIds = f.attrIDs
      val edge = (attrIds(0), attrIds(1))
      (edge, f)
    }.toMap

    println(s"schemas:${schemas}")
    println(s"str:${str}")
//    println(s"edgeToSchema:${edgeToSchemaMap}")

    val hyperNodeExtractRegex = "\\w\\[(\\w-\\w;)+\\]".r
    val hyperEdgeExtractRegex = "\\((\\w-\\w;)+\\)".r
    val edgeExtractRegex = "\\w-\\w;".r

    val hyperNode = hyperNodeExtractRegex
      .findAllMatchIn(str)
      .map { f =>
        val hyperNodeId = f.matched(0).toString.toInt
        val edges = edgeExtractRegex
          .findAllMatchIn(f.matched)
          .map { f =>
            val edge =
              f.matched
                .dropRight(1)
                .split("-")
                .map(f => catalog.registerAttr(f))
            (edge(0), edge(1))
          }
          .toSeq

        (hyperNodeId, edges.map(edgeToSchemaMap))
      }
      .toArray
      .toSeq

    val hyperEdge =
      edgeExtractRegex
        .findAllMatchIn(
          hyperEdgeExtractRegex
            .findFirstMatchIn(str)
            .get
            .matched
        )
        .map { f =>
          val edge = f.matched.dropRight(1).split("-").map(f => f.toInt)
          (edge(0), edge(1))
        }
        .toArray
        .toSeq

    val ghd = RelationGHDTree(hyperNode, hyperEdge, -1.0f)

    ghd
  }

  def stringToGraph(str: String): Graph = {

    val catalog = Catalog.defaultCatalog()
    val edgeExtractRegex = "\\w-\\w;".r
    val E = edgeExtractRegex
      .findAllMatchIn(str)
      .map { m =>
        val edge =
          m.matched.dropRight(1).split("-").map(f => catalog.registerAttr(f))
        (edge(0), edge(1))
      }
      .toArray

    val V = E.flatMap(f => Seq(f._1, f._2)).distinct.toArray

    val g = new Graph(V, E)

    g
  }

  //quadTriangle
  val quadTriangleGraph = "A-B;B-C;C-D;D-E;E-F;F-A;B-D;B-E;B-F;"
  val quadTriangleGHD =
    s"""
       |V(1[A-B;B-F;F-A;],2[B-F;B-E;E-F;],3[B-D;B-E;D-E;],4[B-C;B-D;C-D;])
       |E(1-2;2-3;3-4;)
       |""".stripMargin

  //triangleCore
  private val triangleCoreGraph = "A-B;B-C;C-D;D-E;E-F;F-A;B-D;B-F;D-F;"
  private val triangleCoreGHD =
    s"""
       |V(1[A-B;F-A;B-F;],2[B-D;B-F;D-F;],3[B-C;B-D;C-D;],4[D-E;D-F;E-F;])
       |E(1-2;2-3;2-4;)
       |""".stripMargin

  //twinCSquare
  private val twinCSquareGraph = "A-B;B-C;C-D;D-E;E-F;F-A;A-C;A-D;D-F;"
  private val twinCSquareGHD =
    s"""
       |V(1[A-B;A-C;B-C;],2[A-C;A-D;C-D;],3[F-A;A-D;D-F;],4[D-F;D-E;E-F;])
       |E(1-2;2-3;3-4;)
       |""".stripMargin

  //twinClique4
  private val twinClique4Graph = "A-B;B-C;C-D;D-E;E-F;F-A;A-C;B-F;C-E;C-F;D-F;"
  private val twinClique4GHD =
    s"""
       |V(1[A-B;A-C;F-A;B-C;B-F;C-F;],2[C-D;C-E;C-F;D-E;D-F;E-F;])
       |E(1-2;)
       |""".stripMargin

  //starofDavidPlus
//  private val starofDavidPlusGraph =
//    "A-B;B-C;C-D;D-E;E-F;F-A;A-C;A-E;B-D;B-F;C-E;C-F;D-F;"
//  private val starofDavidPlusGHD = ""

}
