package org.jgrapht.alg.isomorphism

import org.apache.spark.disc.optimization.cost_based.decomposition.graph.Graph._

import scala.collection.mutable.ArrayBuffer

object GraphIsoUtil {

  def isIsomorphic(g1: RawGraph, g2: RawGraph): Boolean = {
    val isoInspector = new VF2GraphIsomorphismInspector(g1, g2)
    isoInspector.isomorphismExists()
  }

  def findAutomorphisms(g: RawGraph): Seq[Mapping] = {
    findIsomorphism(g, g)
  }

  def findIsomorphism(g1: RawGraph, g2: RawGraph): Seq[Mapping] = {
    import scala.collection.JavaConversions._

    val isoInspector = new VF2GraphIsomorphismInspector(g1, g2)
    val mappings = ArrayBuffer[Map[Int, Int]]()
    val mappingIterator = isoInspector.getMappings()
    while (mappingIterator.hasNext()) {
      mappings.add(mappingIterator.next().getForwardMapping().toMap)
    }

    mappings
  }
}
