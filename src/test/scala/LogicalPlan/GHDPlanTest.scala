package LogicalPlan

import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{Relation, RelationSchema}
import org.scalatest.FunSuite

class GHDPlanTest extends FunSuite{

  RelationSchema.reset()

  val data = "./wikiV.txt"
  val relationSchema = RelationSchema.getRelationSchema()

  val relationR1 = Relation("R1",Seq("A","B"),10, data)
  val relationR1WithP = relationR1.toRelationWithP(Seq(6,6))

  val relationR2 = Relation("R2",Seq("B","C"),10, data)
  val relationR2WithP = relationR2.toRelationWithP(Seq(6,6))

  val relationR3 = Relation("R3",Seq("C","D"),10, data)
  val relationR3WithP = relationR3.toRelationWithP(Seq(6,6))

  val relationR4 = Relation("R4",Seq("D","E"),10, data)
  val relationR4WithP = relationR4.toRelationWithP(Seq(6,6))

  val relationR5 = Relation("R5",Seq("E","A"),10, data)
  val relationR5WithP = relationR5.toRelationWithP(Seq(6,6))

  val relationR6 = Relation("R6",Seq("B","E"),10, data)
  val relationR6WithP = relationR6.toRelationWithP(Seq(6,6))


  //    house
  relationSchema.addRelation(relationR1)
  relationSchema.addRelation(relationR2)
  relationSchema.addRelation(relationR3)
  relationSchema.addRelation(relationR4)
  relationSchema.addRelation(relationR5)
  relationSchema.addRelation(relationR6)



//  //    threeTriangle
//  relationSchema.addRelation(Relation("R1",Seq("A","B"),10))
//  relationSchema.addRelation(Relation("R2",Seq("B","C"),10))
//  relationSchema.addRelation(Relation("R3",Seq("C","D"),10))
//  relationSchema.addRelation(Relation("R4",Seq("D","E"),10))
//  relationSchema.addRelation(Relation("R5",Seq("E","A"),10))
//  relationSchema.addRelation(Relation("R6",Seq("A","C"),10))
//  relationSchema.addRelation(Relation("R7",Seq("A","D"),10))

//  test("basic"){
//    val gHDGenerator = new GHDGenerator((0 until relationSchema.relations.size).to[ArrayBuffer])
//    val (optimalGHDTree,_) = gHDGenerator.fhwOptimalGHD()
//    val gHDPlanGenerator = new OrderedGHDPlanGenerator(optimalGHDTree)
//
//
////    println(optimalGHDTree.nodes)
//
//    val orders = gHDPlanGenerator.validOrders()
////    orders.foreach(println)
//
//    val plans = gHDPlanGenerator.generatePlans()
////    plans.foreach(println)
//  }
//
//  test("orderedGHDNode"){
//    val gHDGenerator = new GHDGenerator((0 until relationSchema.relations.size).to[ArrayBuffer])
//    val (optimalGHDTree,_) = gHDGenerator.fhwOptimalGHD()
//    val gHDPlanGenerator = new OrderedGHDPlanGenerator(optimalGHDTree)
//
//
//    println(optimalGHDTree.nodes)
//
//
//    val plans = gHDPlanGenerator.generatePlans()
//    val planArray = plans.plans
//    planArray.foreach(println)
//
//    val firstPlan = planArray(0)
//    val orderedNodes = firstPlan.nodes
//    orderedNodes.foreach(println)
//    orderedNodes.foreach(f => println(f._2.GJOrder().map(relationSchema.getAttribute)))
//    orderedNodes.foreach(f => println(f._2.GJStages()))
//
//    val orderedNode1 =orderedNodes.toSeq(0)._2
//    val orderedNode2 =orderedNodes.toSeq(1)._2
//
//
//    println(orderedNode1)
//    println(orderedNode2)
//
////    logoTest
//    val catalog = LogoCatalog.getCatalog()
//
//
//    val edge = catalog.registorRelationWithP(relationR1WithP)
//    catalog.registorRelationWithP(relationR2WithP)
//    catalog.registorRelationWithP(relationR3WithP)
//    catalog.registorRelationWithP(relationR4WithP)
//    catalog.registorRelationWithP(relationR5WithP)
//    catalog.registorRelationWithP(relationR6WithP)
////
//////    val pattern = orderedNode1.samplingLogo(10)
////    orderedNode1.initInformationSampling(1000)
////
////    println(orderedNode1.sampledCardinality)
////    println(orderedNode1.sampledExecutionTime)
////
////    orderedNode2.initInformationSampling(1000)
////
////    println(orderedNode2.sampledCardinality)
////    println(orderedNode2.sampledExecutionTime)
////    println(orderedNode2.sampledQueryTime)
//
//    plans.allSampleInformGenerate(1000)
//
//    println(orderedNode1.sampledCardinality)
//        println(orderedNode1.sampledExecutionTime)
//
//
//
//        println(orderedNode2.sampledCardinality)
//        println(orderedNode2.sampledExecutionTime)
//        println(orderedNode2.sampledQueryTime)
//
//
//  }
}
