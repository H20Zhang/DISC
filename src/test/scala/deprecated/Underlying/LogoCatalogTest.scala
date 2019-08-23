package deprecated.Underlying

import org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.Structure.{LogoCatalog, Relation, RelationSchema}
import org.scalatest.FunSuite

class LogoCatalogTest extends FunSuite{

  test("basic"){
    val relationSchema = RelationSchema.getRelationSchema()
    val catalog = LogoCatalog.getCatalog()
    val data = "./wikiV.txt"

    val relationR1 = Relation("R1",Seq("A","B"),data)
    val relationR1WithP = relationR1.toRelationWithP(Seq(6,6))
    relationSchema.addRelation(relationR1)
    val edge = catalog.registorRelationWithP(relationR1WithP)

    val edge1 = catalog.retrieveOrRegisterRelationWithP(relationR1.toRelationWithP(Seq(6,6)))
    println(edge1 == edge)

//    println(catalog.retrieveOrRegisterLogo(relationR1.toRelationWithP(Seq(7,7))))






  }
}
