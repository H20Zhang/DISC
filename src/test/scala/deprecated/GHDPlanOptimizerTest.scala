package deprecated

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.adj.utlis.SparkSingle
import org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.GHDOptimize.{GHDGenerator, GHDPlanOptimizer}
import org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.Structure.{Configuration, Relation, RelationSchema}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class GHDPlanOptimizerTest extends FunSuite{




  val data = "./wikiV.txt"
  val relationSchema = RelationSchema.getRelationSchema()

  //house
  relationSchema.addRelation(Relation("R1",Seq("A","B"),data, 201526))
  relationSchema.addRelation(Relation("R2",Seq("B","C"),data, 201526))
  relationSchema.addRelation(Relation("R3",Seq("C","D"),data, 201526))
  relationSchema.addRelation(Relation("R4",Seq("D","E"),data, 201526))
  relationSchema.addRelation(Relation("R5",Seq("E","A"),data, 201526))
  relationSchema.addRelation(Relation("R6",Seq("B","E"),data, 201526))


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
    val conf = Configuration.getConfiguration()
    val relationSchema = RelationSchema.getRelationSchema()
    val generator = new GHDGenerator((0 until relationSchema.relations.size).to[ArrayBuffer])
    val optimizer = new GHDPlanOptimizer(generator.fhwOptimalGHD()._1)



    val spark = SparkSingle.getSparkSession()

    val log = LogManager.getLogger(this.getClass)
    log.log(Level.toLevel(25000),"testing")

    val tree = optimizer.tree
    println(tree)
    println(tree.nodes)

    val node0 = tree.nodes.values.toSeq(0)
    val node1 = tree.nodes.values.toSeq(1)

//    val informationSampler = new InformationSampler(tree, conf.defaultK)
//    informationSampler.sizeTimeMap(node0.id) = (3679018,1571,216)
//    informationSampler.sizeTimeMap(node1.id) = (520015534,84056,1296)
//    informationSampler.queryTimeMap((node1.id,node0.id)) = (422683,62,216)
//    informationSampler.queryTimeMap((node0.id,node1.id)) = (57795182,7793,1296)

//    optimizer.setInformationSampler(informationSampler)

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

    val firstPlan = plans(0)
    println(firstPlan)
    val cost = firstPlan.costEstimation()
    println(cost)

    val bestPlan = optimizer.genBestPlan()
    val plansWithCost = plans.map(f => (f,f.costEstimation()))

    plansWithCost.foreach(println)

    println(bestPlan)
    println(optimizer.informationSampler)

    val subPattern = bestPlan._1.assemble()
    val size = subPattern.logo.size()
    println(size)



  }


}
