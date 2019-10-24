package adj

import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.optimization.costBased.decomposition.relationGraph.{
  HyperTreeDecomposer,
  RelationDecomposer,
  RelationEdge,
  RelationGraph
}
import org.scalatest.FunSuite

class HyperTreeDecompositionTest extends FunSuite {

  test("graph decomposition") {
//    org.apache.spark.adj.adj.utils.decomposition.graph.HyperTreeDecomposer.test()
  }

  test("RelationGraph") {

    val edge1 = Set(0, 1, 3)
    val edge2 = Set(0, 2, 4)
    val edge3 = Set(1, 2, 5)
    val E = Seq(edge1, edge2, edge3).map(RelationEdge)
    val V = E.flatMap(_.attrs).distinct
    val graph1 = RelationGraph(V, E)

    println(s"graph1.isConnected:${graph1.isConnected()}")
    println(s"graph1.containNode(${0}):${graph1.containNode(0)}")
    println(s"graph1.width:${graph1.width()}")

    val edge4 = Set(2, 4)
    val edge5 = Set(3, 4, 5)
    val E2 = Seq(edge4, edge5).map(RelationEdge)
    val V2 = E2.flatMap(_.attrs).distinct
    val graph2 = RelationGraph(V, E2)

    println(s"graph2 is subgraph of graph1:${graph1.containSubgraph(graph2)}")
  }

  test("RelationGraph -- Decomposition") {
    val edge1 = Set(0, 1)
    val edge2 = Set(0, 2)
    val edge3 = Set(1, 2)
    val edge4 = Set(1, 3)
    val edge5 = Set(2, 3)
    val edge6 = Set(3, 4)
    val E = Seq(edge1, edge2, edge3, edge4, edge5, edge6).map(RelationEdge)
    val V = E.flatMap(_.attrs).distinct
    val graph1 = RelationGraph(V, E)

    val ghds = HyperTreeDecomposer.allGHDs(graph1)

    ghds.foreach(println)
    println(s"number of GHD is:${ghds.size}")
  }

  test("Relation -- Tree Decomposition") {
    val edge0 = Set(0)
    val edge1 = Set(0, 1)
    val edge2 = Set(0, 2)
    val edge3 = Set(1, 2)
    val edge4 = Set(1, 3)
    val edge5 = Set(2, 3)
    val edge6 = Set(3, 4)
    val edge7 = Set(2, 4)

    val schemas =
      Seq(edge0, edge1, edge2, edge3, edge4, edge5, edge6, edge7).zipWithIndex
        .map {
          case (attrs, id) =>
            val schema = RelationSchema(s"R${id}", attrs.map(_.toString).toSeq);
            schema.register()
            schema
        }

    val decomposer = new RelationDecomposer(schemas)
    val relationGHD = decomposer.decomposeTree().head
    println(s"relationGHD:${relationGHD}")
  }

  test("Relation -- Star Decomposition") {
    val schemaR0 = RelationSchema("R0", Seq("A", "B"))
    val schemaR1 = RelationSchema("R1", Seq("B", "C"))
    val schemaR2 = RelationSchema("R2", Seq("C", "D"))
    val schemaR3 = RelationSchema("R3", Seq("D", "A"))
    val schemaR4 = RelationSchema("R4", Seq("A", "E"))
    val schemaR5 = RelationSchema("R5", Seq("B", "E"))
    val schemaR6 = RelationSchema("R6", Seq("C", "E"))
    val schemaR7 = RelationSchema("R7", Seq("D", "E"))

    val schemas =
      Seq(
        schemaR0,
        schemaR1,
        schemaR2,
        schemaR3,
        schemaR4,
        schemaR5,
        schemaR6,
        schemaR7
      )
    schemas.foreach(_.register())

    val decomposer = new RelationDecomposer(schemas)
    val relationGHDs = decomposer.decomposeTree()
//    println(s"relationGHD:${relationGHDs}")

    relationGHDs.foreach { star =>
      if (Math.ceil(star.fhtw).toInt == 2 && star.V.exists(
            node =>
              node._2
                .map(_.name)
                .toSet == Seq("R0", "R3", "R4", "R5", "R7").toSet
          )) {
        println()
        println(star)
      }
    }

  }
}
