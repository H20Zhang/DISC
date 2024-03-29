package org.apache.spark

import org.apache.log4j.Logger
import org.apache.spark.disc.catlog.Catalog
import org.jgrapht.graph.{DefaultEdge, DefaultUndirectedGraph}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

package object disc {

  type RawGraph = DefaultUndirectedGraph[Int, DefaultEdge]
  type Mapping = Map[NodeID, NodeID]

// normal graph
  type NodeID = Int
  type Edge = (NodeID, NodeID)
  type NodeList = Seq[NodeID]
  type EdgeList = Seq[Edge]
  type ADJList = mutable.HashMap[NodeID, ArrayBuffer[NodeID]]

// hypertree, where a hypernode is a graph
  type GraphID = Int

  trait TestAble {
    def test(): Unit
  }

  trait IsomorphicAble {
    def isIsomorphic(p: IsomorphicAble): Boolean

    def findIsomorphism(p: IsomorphicAble): Seq[Mapping]

    def findIsomorphismUnderConstriant(p: IsomorphicAble,
                                       constraint: Mapping): Seq[Mapping]

    def findAutomorphism(): Seq[Mapping]

    def findAutomorphismUnderConstriant(constraint: Mapping): Seq[Mapping]
  }

  trait LogAble {
    org.apache.log4j.PropertyConfigurator
      .configure("./src/resources/log4j.preperties")

    lazy val logger: Logger = Logger.getLogger(this.getClass)
  }

  object Stage extends Enumeration {
    type Stage = Value
    val Unprocessed, SymmetryBreaked, NonInduceInstanceRemoved,
    NonIsomorphismRemoved, Decomposed, Optimized = Value
  }

  object CountAttrCounter {
    var count = 0
    def nextCountAttrId() = {
      val old = count
      count += 1
      val countAttr = s"Count${old}"
      val catalog = Catalog.defaultCatalog()

      val id = catalog.registerAttr(countAttr)

      id
    }
  }

}
