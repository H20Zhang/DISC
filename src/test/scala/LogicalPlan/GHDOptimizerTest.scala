package LogicalPlan

import org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize.SubSetsGenerator
import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{Relation, RelationSchema}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class GHDOptimizerTest extends FunSuite{


  test("subsetGenerator"){


    val relationSchema = RelationSchema.getRelationSchema()

    relationSchema.addRelation(Relation("R1",Seq("A","B"),1))
    relationSchema.addRelation(Relation("R2",Seq("B","C"),1))
    relationSchema.addRelation(Relation("R3",Seq("C","D"),1))
    relationSchema.addRelation(Relation("R4",Seq("A","D"),1))
    relationSchema.addRelation(Relation("R5",Seq("A","E"),1))
    relationSchema.addRelation(Relation("R6",Seq("D","E"),1))

    val generator = new SubSetsGenerator((0 until relationSchema.relations.size).to[ArrayBuffer])

    val subsets = generator.enumerateSet()

    subsets.foreach{f =>
      println()
      f.foreach{w =>
        print("|")
        w.foreach(u => print(s"${relationSchema.getRelation(u).name} "))}
    }

    println(subsets.size)

    val x = 1


  }


}
