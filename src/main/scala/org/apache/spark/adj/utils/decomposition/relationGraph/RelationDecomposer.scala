package org.apache.spark.adj.utils.decomposition.relationGraph

import org.apache.spark.adj.database.Catalog.RelationID
import org.apache.spark.adj.database.RelationSchema

//TODO: few more test needed
class RelationDecomposer(schemas: Seq[RelationSchema]) {
  def decompose(): IndexedSeq[RelationGHD] = {

    //filter the schemas that are contained inside another schema
    val containedSchemas = schemas.filter { s1 =>
      schemas.diff(Seq(s1)).exists(s2 => s1.attrIDs.diff(s2.attrIDs).isEmpty)
    }
    val notContainedSchemas = schemas.diff(containedSchemas)

    //find the GHD Decomposition for the notContainedSchemas
    val E = notContainedSchemas.map(f => RelationEdge(f.attrIDs.toSet))
    val V = E.flatMap(_.attrs).distinct
    val graph = RelationGraph(V, E)
    val ghds = HyperTreeDecomposer.allGHDs(graph)

    //construct RelationGHD
    ghds
      .map { t =>
        val edgeToSchema =
          notContainedSchemas.map(f => (f.attrIDs.toSet, f)).toMap
        val bags =
          t.V
            .map(f => (f.id, f.g.E().map(edge => edgeToSchema(edge.attrs))))
            .map {
              case (idx, bag) =>
                //add previously filtered schemas to the bags that contained it.
                val fullBag = bag ++ bag
                  .flatMap(
                    schema1 =>
                      containedSchemas.filter(
                        schema2 => schema2.attrIDs.diff(schema1.attrIDs).isEmpty
                    )
                  )
                  .distinct

                (idx, fullBag)
            }
        val connections = t.E.map(e => (e.u.id, e.v.id))

        RelationGHD(bags, connections, t.fractionalHyperNodeWidth())
      }
      .sortBy(relationGHD => (relationGHD.fhtw, -relationGHD.E.size))
  }
}

case class RelationGHD(V: Seq[(Int, Seq[RelationSchema])],
                       E: Seq[(Int, Int)],
                       fhtw: Double) {}
