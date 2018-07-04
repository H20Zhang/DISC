package LogicalPlan

import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{Relation, RelationSchema}
import org.apache.spark.Logo.UnderLying.utlis.TestUtil
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class StructureTest extends FunSuite{

  val relationSchema = RelationSchema.getRelationSchema()

  relationSchema.addRelation(Relation("R1",Seq("A","B"),1))
  relationSchema.addRelation(Relation("R2",Seq("B","C"),1))
  relationSchema.addRelation(Relation("R3",Seq("C","D"),1))
  relationSchema.addRelation(Relation("R4",Seq("A","D"),1))
  relationSchema.addRelation(Relation("R5",Seq("A","E"),1))
  relationSchema.addRelation(Relation("R6",Seq("D","E"),1))


  test("RelationSchemaTest"){


    val relationSchema1 = RelationSchema.getRelationSchema()

    assert(TestUtil.listEqual(relationSchema1.attributes, Seq("A","B","C","D","E")))
    assert(TestUtil.listEqual(relationSchema1.relations.map(_.name),Seq("R1","R2","R3","R4","R5")))

    val test = relationSchema.getRelationId(ArrayBuffer("A","B"))
    println(s"${test.get}")
  }

  test("RelationSchemaGetRelationTest"){
    println(relationSchema.getInducedRelation(Seq("A","B","C","D")))
  }
}
