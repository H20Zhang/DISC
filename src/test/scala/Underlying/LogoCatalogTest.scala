package Underlying

import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{LogoCatalog, Relation, RelationSchema}
import org.scalatest.FunSuite

class LogoCatalogTest extends FunSuite{

  test("basic"){
    val relationSchema = RelationSchema.getRelationSchema()
    val catalog = LogoCatalog.getCatalog()
    val data = "./wikiV.txt"

    val relationR1 = Relation("R1",Seq("A","B"))
    val relationR1WithP = relationR1.toRelationWithP(Seq(6,6))
    relationSchema.addRelation(relationR1)
    val edge = catalog.registorRelationFromAdress(relationR1WithP, data)

    val edge1 = catalog.retrieveLogo(relationR1.toRelationWithP(Seq(6,6))).get
    println(edge1 == edge)

    println(catalog.retrieveLogo(relationR1.toRelationWithP(Seq(7,7))).isDefined)






  }
}
