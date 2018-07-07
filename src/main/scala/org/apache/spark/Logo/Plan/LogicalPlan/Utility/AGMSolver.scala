package org.apache.spark.Logo.Plan.LogicalPlan.Utility

import cern.colt.matrix.tdouble.impl.DenseDoubleMatrix2D
import com.joptimizer.optimizers.{LPOptimizationRequest, LPPrimalDualMethod}
import org.apache.spark.Logo.Plan.LogicalPlan.Structure.RelationSchema

import scala.math.BigDecimal.RoundingMode

object AGMSolver {

  def linearProgramm(relationIDs:Seq[Int]) = {
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


  def AGMOptimalFractionEdgeCover(relationIDs:Seq[Int]): Array[Double] ={

    val schema = RelationSchema.getRelationSchema()
    val relations = relationIDs.map(schema.getRelation)
    val cardinalities = relations.map(_.cardinality)
    val attributes = relations.flatMap(_.attributes).distinct

    val c = cardinalities.map(f => Math.log(f)).toArray

//    val G = new Array[Array[Double]](attributes.size)

    val G = new DenseDoubleMatrix2D(attributes.size,c.size)


    for (i <- 0 until attributes.size){
      for(j <- 0 until relations.size){
        if (relations(j).attributes.contains(attributes(i))){

          G.set(i,j,-1.0)
//          temp(j) = -1.0
        } else{
          G.set(i,j,0.0)
        }
      }
    }

    val h = Array.fill(attributes.size)(-1.0)
    val lb = Array.fill(c.size)(0.0)
    val ub = Array.fill(c.size)(1.01)

    val or = new LPOptimizationRequest

    val opt= new LPPrimalDualMethod
    or.setC(c)
    or.setG(G)
    or.setH(h)
    or.setLb(lb)
    or.setUb(ub)
    or.setDumpProblem(false)
    opt.setLPOptimizationRequest(or)
    opt.optimize()



    val sol = opt.getOptimizationResponse.getSolution
//    Math.ulp()

    sol.map(f => BigDecimal.valueOf(f).setScale(3,RoundingMode.HALF_EVEN).doubleValue())
  }

  def solveAGMBound(relationIDs:Seq[Int]):Double = {
    val schema = RelationSchema.getRelationSchema()
    val relations = relationIDs.map(schema.getRelation)
    val cardinalities = relations.map(_.cardinality)

    val sol = AGMOptimalFractionEdgeCover(relationIDs)
    cardinalities.zip(sol).map(f => Math.pow(f._1,f._2)).product
  }
}
