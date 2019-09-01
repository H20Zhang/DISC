package utils

import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.utils.decomposition.relationGraph.{
  HyperTreeDecomposer,
  RelationDecomposer,
  RelationEdge,
  RelationGraph
}
import org.scalatest.FunSuite

class HyperTreeDecompositionTest extends FunSuite {

  test("graph decomposition") {
    org.apache.spark.adj.utils.decomposition.graph.HyperTreeDecomposer.test()
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

  test("Relation -- Decomposition") {
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
    val relationGHD = decomposer.decompose().head
    println(s"relationGHD:${relationGHD}")
  }
}
