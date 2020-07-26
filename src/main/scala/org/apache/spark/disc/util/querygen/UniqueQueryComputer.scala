package org.apache.spark.disc.util.querygen

import org.apache.spark.disc.optimization.rule_based.subgraph.Pattern
import org.apache.spark.disc.util.misc.Graph

import scala.collection.mutable.ArrayBuffer

class UniqueQueryComputer(numNode: Int) {

  def genValidPattern(): Seq[Graph] = {
    println(s"start computing numbers of valid pattern with $numNode-node")

    val V = Range(0, numNode).toArray
    val potentialE = V.combinations(2).map(f => (f(0), f(1))).toArray

    //all graph
    val uniqueGraphs = Range(0, potentialE.size + 1).par.flatMap { numEdge =>
      val connectedGraphs = potentialE
        .combinations(numEdge)
        .map { E =>
          val g = new Graph(V, E)
          g
        }
        .filter(g => g.isConnected())

      val _uniqueGraphs = ArrayBuffer[Graph]()
      connectedGraphs.foreach { g1 =>
        if (!_uniqueGraphs.exists(g2 => g2.isIsomorphic(g1))) {
          _uniqueGraphs += g1
        }
      }
      _uniqueGraphs
    }.toArray

    uniqueGraphs
  }

  def genValidQuery(): Seq[Pattern] = {
    println(
      s"start computing numbers of valid node-centric-query with $numNode-node"
    )

    val uniqueGraphs = genValidPattern()
//    val uniquePatterns = ArrayBuffer[Pattern]()
    val uniquePatterns = uniqueGraphs.flatMap { g =>
      val V = g.V
      val E = g.E
      val localUniquePattern = ArrayBuffer[Pattern]()

      //      node pair orbit

//      V.combinations(2)
//        .map(f => (f(0), f(1)))
//        .flatMap(f => Seq(f, f.swap))
//        .toSeq
//        .diff(E.flatMap(f => Seq(f, f.swap)))
//        .map {
//          case (u, v) =>
//            val C = Array(u, v)
//            new Pattern(V, E, C)
//        }
//        .foreach { p1 =>
//          val needAdding = !localUniquePattern.exists { p2 =>
//            val g1 = p1.toGraph
//            val g2 = p2.toGraph
//            val mappings = g1.findIsomorphism(g2)
//            mappings.exists(
//              mapping =>
//                mapping(p1.C(0)) == p2.C(0) && mapping(p1.C(1)) == p2
//                  .C(1)
//            )
//          }
//
//          if (needAdding) {
//            localUniquePattern += p1
//          }
//        }

//      //node orbit
      V.map { v =>
          val C = Array(v)
          new Pattern(V, E, C)
        }
        .foreach { p1 =>
          val needAdding = !localUniquePattern.exists { p2 =>
            val g1 = p1.toGraph
            val g2 = p2.toGraph
            val mappings = g1.findIsomorphism(g2)
            mappings.exists(mapping => mapping(p1.C(0)) == p2.C(0))
          }

          if (needAdding) {
            localUniquePattern += p1
          }
        }

//      edge orbit
//      E.map {
//          case (u, v) =>
//            val C = Array(u, v)
//            new Pattern(V, E, C)
//        }
//        .foreach { p1 =>
//          val needAdding = !localUniquePattern.exists { p2 =>
//            val g1 = p1.toGraph
//            val g2 = p2.toGraph
//            val mappings = g1.findIsomorphism(g2)
//            mappings.exists(
//              mapping =>
//                mapping(p1.C(0)) == p2.C(0) && mapping(p1.C(1)) == p2
//                  .C(1)
//            )
//          }
//
//          if (needAdding) {
//            localUniquePattern += p1
//          }
//        }

//      val triangle = new Graph(Seq(0, 1, 2), Seq((0, 1), (1, 2), (0, 2)))
//      //triangle orbit
//      E.combinations(3)
//        .filter {
//          case edgeSet =>
//            val nodes = edgeSet.flatMap(f => Seq(f._1, f._2)).distinct
//            val newGraph = new Graph(nodes, edgeSet)
//            newGraph.isIsomorphic(triangle)
//        }
//        .map {
//          case edgeSet =>
//            val nodes = edgeSet.flatMap(f => Seq(f._1, f._2)).distinct
//            new Pattern(V, E, nodes)
//        }
//        .foreach { p1 =>
//          val needAdding = !localUniquePattern.exists { p2 =>
//            val g1 = p1.toGraph
//            val g2 = p2.toGraph
//            val mappings = g1.findIsomorphism(g2)
//            mappings.exists(
//              mapping =>
//                mapping(p1.C(0)) == p2.C(0) && mapping(p1.C(1)) == p2
//                  .C(1) && mapping(p1.C(2)) == p2
//                  .C(2)
//            )
//          }
//
//          if (needAdding) {
//            localUniquePattern += p1
//          }
//        }
//
      localUniquePattern
    }

    uniquePatterns
  }

}
