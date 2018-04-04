package org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize

import com.joptimizer.optimizers.{LPOptimizationRequest, LPPrimalDualMethod}
import org.apache.spark.Logo.Plan.LogicalPlan.Structure.RelationSchema

import scala.collection.mutable.ArrayBuffer

class AGMSolver(opt:LPPrimalDualMethod) {

  def linearProgramm(relationIDs:ArrayBuffer[Int]) = {
    val schema = RelationSchema.getRelationSchema()
    val relations = relationIDs.map(schema.getRelation)
    val cardinalities = relations.map(_.cardinality)
    val attributes = relations.flatMap(_.attributes).distinct


    val c = cardinalities.map(f => Math.log(f))
    val G = new Array[Array[Double]](attributes.size)

    for (i <- 0 until G.size){
      val temp = Array.fill(c.size)(0.0)
      G(i) = temp

      for(j <- 0 until relations.size){
        if (relations(j).attributes.contains(attributes(i))){
          temp(j) = -1.0
        }
      }
    }

    val h = Array.fill(G.size)(-1.0)
    val lb = Array.fill(c.size)(0.0)
    val ub = Array.fill(c.size)(1.01)

    println()
    println("c")
    c.foreach(f => print(s"$f "))

    println()
    println("G")
    G.foreach{f =>
      println()
      f.foreach(w => print(s"$w "))
    }

    println()
    println("h")
    h.foreach(f => print(s"$f "))

    println()
    println("lb")
    lb.foreach(f => print(s"$f "))

    println()
    println("ub")
    ub.foreach(f => print(s"$f "))
  }


  def AGMOptimalFractionEdgeCover(relationIDs:ArrayBuffer[Int]): Array[Double] ={

    val schema = RelationSchema.getRelationSchema()
    val relations = relationIDs.map(schema.getRelation)
    val cardinalities = relations.map(_.cardinality)
    val attributes = relations.flatMap(_.attributes).distinct


    val c = cardinalities.map(f => Math.log(f)).toArray
    val G = new Array[Array[Double]](attributes.size)

    for (i <- 0 until G.size){
      val temp = Array.fill(c.size)(0.0)
      G(i) = temp

      for(j <- 0 until relations.size){
        if (relations(j).attributes.contains(attributes(i))){
          temp(j) = -1.0
        }
      }
    }

    val h = Array.fill(G.size)(-1.0)
    val lb = Array.fill(c.size)(0.0)
    val ub = Array.fill(c.size)(1.01)

    val or = new LPOptimizationRequest
    or.setC(c)
    or.setG(G)
    or.setH(h)
    or.setLb(lb)
    or.setUb(ub)
    or.setDumpProblem(true)
    opt.setLPOptimizationRequest(or)
    opt.optimize()

    val sol = opt.getOptimizationResponse.getSolution
    sol
  }

  def solveAGMBound(relationIDs:ArrayBuffer[Int]):Double = {
    val schema = RelationSchema.getRelationSchema()
    val relations = relationIDs.map(schema.getRelation)
    val cardinalities = relations.map(_.cardinality)

    val sol = AGMOptimalFractionEdgeCover(relationIDs)
    cardinalities.zip(sol).map(f => Math.pow(f._1,f._2)).sum
  }
}
