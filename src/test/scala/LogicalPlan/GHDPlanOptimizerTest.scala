package LogicalPlan

import org.apache.log4j.LogManager
import org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize.{GHDGenerator, GHDPlanOptimizer}
import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{Relation, RelationSchema}
import org.apache.spark.Logo.UnderLying.utlis.SparkSingle
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class GHDPlanOptimizerTest extends FunSuite{




  val data = "./wikiV.txt"
  val relationSchema = RelationSchema.getRelationSchema()

  //house
  relationSchema.addRelation(Relation("R1",Seq("A","B"),data))
  relationSchema.addRelation(Relation("R2",Seq("B","C"),data))
  relationSchema.addRelation(Relation("R3",Seq("C","D"),data))
  relationSchema.addRelation(Relation("R4",Seq("D","E"),data))
  relationSchema.addRelation(Relation("R5",Seq("E","A"),data))
  relationSchema.addRelation(Relation("R6",Seq("B","E"),data))


  //threeTriangle
//      relationSchema.addRelation(Relation("R1",Seq("A","B"),50000))
//      relationSchema.addRelation(Relation("R2",Seq("B","C"),10000))
//      relationSchema.addRelation(Relation("R3",Seq("C","D"),600000))
//      relationSchema.addRelation(Relation("R4",Seq("D","E"),8000))
//      relationSchema.addRelation(Relation("R5",Seq("E","A"),100000))
//      relationSchema.addRelation(Relation("R6",Seq("A","C"),5000000))
//      relationSchema.addRelation(Relation("R7",Seq("A","D"),1000000))

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


  test("GHDOptimizer"){
    val relationSchema = RelationSchema.getRelationSchema()
    val generator = new GHDGenerator((0 until relationSchema.relations.size).to[ArrayBuffer])
    val optimizer = new GHDPlanOptimizer(generator.fhwOptimalGHD()._1)

    val spark = SparkSingle.getSparkSession()

    val log = LogManager.getLogger(this.getClass)
    log.error("testing")

    val tree = optimizer.tree
    println(tree)
    println(tree.nodes)

    val ps = optimizer.genPs()
    ps.foreach(println)
    println(ps.size)

    val orders = optimizer.genOrders()
    orders.foreach(println)
    println(orders.size)

    val lazyMappings = optimizer.genLazyMappings()
    lazyMappings.foreach(println)
    println(lazyMappings.size)

    val plans = optimizer.genPlans()
    println(plans.size)

//    val firstPlan = plans(0)
//    println(firstPlan)
//    val cost = firstPlan.costEstimation()
//    println(cost)

    val bestPlan = optimizer.genBestPlan()
    println(bestPlan)

    println(optimizer.informationSampler)




  }


}
