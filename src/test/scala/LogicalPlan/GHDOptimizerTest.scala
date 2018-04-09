package LogicalPlan

import org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize.{GHDOptimizer, SubSetsGenerator}
import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{Relation, RelationSchema}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class GHDOptimizerTest extends FunSuite{




  val relationSchema = RelationSchema.getRelationSchema()

  //house
  relationSchema.addRelation(Relation("R1",Seq("A","B"),1000000))
  relationSchema.addRelation(Relation("R2",Seq("B","C"),1000000))
  relationSchema.addRelation(Relation("R3",Seq("C","D"),1000000))
  relationSchema.addRelation(Relation("R4",Seq("D","E"),1000000))
  relationSchema.addRelation(Relation("R5",Seq("E","A"),1000000))
  relationSchema.addRelation(Relation("R6",Seq("B","E"),500000))


  //threeTriangle
  //    relationSchema.addRelation(Relation("R1",Seq("A","B"),50000))
  //    relationSchema.addRelation(Relation("R2",Seq("B","C"),10000))
  //    relationSchema.addRelation(Relation("R3",Seq("C","D"),600000))
  //    relationSchema.addRelation(Relation("R4",Seq("D","E"),8000))
  //    relationSchema.addRelation(Relation("R5",Seq("E","A"),100000))
  //    relationSchema.addRelation(Relation("R6",Seq("A","C"),5000000))
  //    relationSchema.addRelation(Relation("R7",Seq("A","D"),1000000))

  //near5Clique
  //        relationSchema.addRelation(Relation("R1",Seq("A","B"),50000))
  //        relationSchema.addRelation(Relation("R2",Seq("B","C"),10000))
  //        relationSchema.addRelation(Relation("R3",Seq("C","D"),600000))
  //        relationSchema.addRelation(Relation("R4",Seq("D","E"),8000))
  //        relationSchema.addRelation(Relation("R5",Seq("E","A"),100000))
  //        relationSchema.addRelation(Relation("R6",Seq("B","E"),5000000))
  //        relationSchema.addRelation(Relation("R7",Seq("C","E"),1000000))
  //    relationSchema.addRelation(Relation("R8",Seq("B","D"),1000000))
  //
  //



  test("subsetGenerator"){
    val generator = new SubSetsGenerator((0 until relationSchema.relations.size).to[ArrayBuffer])
//
//    val subsets = generator.enumerateSet()
//
//    subsets.foreach{f =>
//      println()
//      f.foreach{w =>
//        print("|")
//        w.foreach(u => print(s"${relationSchema.getRelation(u).name} "))}
//    }
//
//    println(subsets.size)


    generator.printAllRelaxedGHDPlans()
//    generator.relaxedGHDSet()
  }

  test("LeftDeepTree"){
    val gHDOptimizer = new GHDOptimizer((0 until relationSchema.relations.size).to[ArrayBuffer])
    val trees = gHDOptimizer.sampleLeftDeepTrees()

    trees.foreach(f => println(f.treeString()))

    val x = 1
  }


}
