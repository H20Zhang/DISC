package Example

import com.joptimizer.optimizers.LPPrimalDualMethod
import org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize.AGMSolver
import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{Relation, RelationSchema}
import org.scalatest.FunSuite
import scpsolver.lpsolver.SolverFactory
import scpsolver.problems.LPWizard

import scala.collection.mutable.ArrayBuffer

class LPTest extends FunSuite{

  test("JOptimizer"){
    import com.joptimizer.optimizers.LPOptimizationRequest
    import com.joptimizer.optimizers.LPPrimalDualMethod
    //Objective function//Objective function

    //optimization
    val opt = new LPPrimalDualMethod

    for (i <- 0 to 0){

      val r1 = Math.log(4)
      val r2 = Math.log(4)
      val r3 = Math.log(4)

      val c = Array[Double](r1, r2, r3)

      //Inequalities constraints
      val G = Array[Array[Double]](Array(-1, -1,0), Array(-1, 0 , -1), Array(0, -1 , -1))
      val h = Array[Double](-1, -1, -1)

      //Bounds on variables
      val lb = Array[Double](0, 0, 0)
      val ub = Array[Double](1.01, 1.01, 1.01)

      //optimization problem
      val or = new LPOptimizationRequest
      or.setC(c)
      or.setG(G)
      or.setH(h)
      or.setLb(lb)
      or.setUb(ub)
      or.setDumpProblem(true)
//      or.setAvoidPresolvingIncreaseSparsity(true)



      opt.setLPOptimizationRequest(or)
      opt.optimize()


      val sol = opt.getOptimizationResponse.getSolution

//      sol.foreach(f => print(s"$f "))
    }
  }


  test("AGMSolver"){
    val opt = new LPPrimalDualMethod
    val relationSchema = RelationSchema.getRelationSchema()

    relationSchema.addRelation(Relation("R1",Seq("A","B"),1000000))
    relationSchema.addRelation(Relation("R2",Seq("B","C"),1000000))
    relationSchema.addRelation(Relation("R3",Seq("C","D"),1000000))
    relationSchema.addRelation(Relation("R4",Seq("A","D"),500000))
    relationSchema.addRelation(Relation("R5",Seq("A","E"),1000000))
    relationSchema.addRelation(Relation("R6",Seq("D","E"),1000000))

    val solver = new AGMSolver()

    for (i <- 0 until 1){
      val problem = ArrayBuffer(0,1,2,3)
      val sol = solver.AGMOptimalFractionEdgeCover(problem)
      val AGMBound = solver.solveAGMBound(problem)
      solver.linearProgramm(problem)
      sol.foreach(f => print(s"$f "))
      println()
      println(AGMBound)
    }


//    sol.foreach(f => print(s"$f "))
  }
}
