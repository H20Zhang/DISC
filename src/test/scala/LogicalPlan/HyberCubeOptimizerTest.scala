package LogicalPlan

import org.apache.spark.adj.plan.deprecated.LogicalPlan.Decrapted.HyberCubeOptimize.{HyberCubeOptimizer, PGenerator}
import org.scalatest.FunSuite

import scala.collection.mutable

class HyberCubeOptimizerTest extends FunSuite{

  test("pGenerator"){
    val pGenerate = new PGenerator(224*12,5,p = {f => f.product > 224})
    val Ps = pGenerate.generateAllP()

    println(Ps.size)
//    Ps.foreach{f =>
//      f.foreach(h => print(s"$h "))
//      println()
//    }
  }

  test("HyberCubeOptimizer") {


    //triangle
    val triangle = Array(
      (Array(0, 1), 1),
      (Array(0, 2), 1),
      (Array(1, 2), 1)
    )

    //square
    val square = Array(
      (Array(0, 1), 1),
      (Array(1, 2), 1),
      (Array(2, 3), 1),
      (Array(3, 0), 1)
    )

    //chordalSquare
    val chordalSquare = Array(
      (Array(0, 1), 1),
      (Array(1, 2), 1),
      (Array(2, 3), 1),
      (Array(3, 0), 1),
      (Array(0, 2), 1)
    )

    //fourClique
    val fourClique = Array(
      (Array(0, 1), 1),
      (Array(1, 2), 1),
      (Array(2, 3), 1),
      (Array(3, 0), 1),
      (Array(0, 2), 1),
      (Array(1, 3), 1)
    )

    //house
    val house = Array(
      (Array(0, 1), 1.37),
      (Array(1, 2), 1.37),
      (Array(2, 3), 1.37),
      (Array(3, 4), 1.37),
      (Array(4, 0), 1.37),
      (Array(2, 4), 1.37)
    )

    val threeTriangle = Array(
      (Array(0, 1), 1.37),
      (Array(1, 2), 1.37),
      (Array(2, 0), 1.37),
      (Array(1,2,4), 43.36),
      (Array(2, 3), 1.37),
      (Array(3, 4), 1.37)
    )

    val edge = 1.37
    val triangle1 = 6.8
    val givenTriangle = 1.13
    val wedge = 8328.0

    val near5Clique = Array(
      (Array(0, 1), 1.37),
      (Array(1, 2), 1.37),
      (Array(2, 3), 1.37),
      (Array(3, 4), 1.37),
      (Array(4, 0), 1.37),
      (Array(2, 4), 1.37),
      (Array(0, 2), 1.37),
      (Array(1, 4), 1.37)
    )

    val p1 = Array(
      (Array(0, 1), edge),
      (Array(1, 2), edge),
      (Array(2, 3), edge),
      (Array(3, 0), edge),
      (Array(3, 4), edge),
      (Array(2, 4), edge),
      (Array(1, 5), edge),
      (Array(2, 5), edge),
      (Array(1,2,3), givenTriangle)
    )

    val p2 = Array(
      (Array(0, 3), edge),
      (Array(1, 3), edge),
      (Array(0, 2), edge),
      (Array(1, 2), edge),
      (Array(0, 2), triangle1),
      (Array(1, 2), triangle1),
      (Array(1,2,3), givenTriangle)
    )

    val p3 = Array(
      (Array(0, 1, 2),givenTriangle),
      (Array(0,1), wedge),
      (Array(0, 2), triangle1),
      (Array(1, 2), triangle1)
    )


    val hyberCubeOptimizer = new HyberCubeOptimizer(p3 , 224 * 50, 224 * 60, 3, 0.75)
    val possiblePlans = hyberCubeOptimizer.allPlans()

    if (!possiblePlans.isEmpty){
      val minCostPlan = possiblePlans.minBy(f => f.totalCost)

      println(minCostPlan.P)
      println(minCostPlan.totalCost)
      println(minCostPlan.Load)
    }

  }
}
