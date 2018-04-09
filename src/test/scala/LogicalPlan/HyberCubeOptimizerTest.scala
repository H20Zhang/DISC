package LogicalPlan

import org.apache.spark.Logo.Plan.LogicalPlan.HyberCubeOptimize.{HyberCubeOptimizer, PGenerator}
import org.scalatest.FunSuite

class HyberCubeOptimizerTest extends FunSuite{

  test("pGenerator"){
    val pGenerate = new PGenerator(729,5,p = {f => f.product > 224})
    val Ps = pGenerate.generateAllP()

    println(Ps.size)
//    Ps.foreach{f =>
//      f.foreach(h => print(s"$h "))
//      println()
//    }
  }

  test("HyberCubeOptimizer"){


    //house
    val house = Array(
      (Array(0,1),2),
      (Array(0,2),2),
      (Array(1,2),1),
      (Array(1,3),2),
      (Array(2,4),2),
      (Array(3,4),2)
    )

    val threeTriangle = Array(
      (Array(1,3),100),
      (Array(0,3),100),
      (Array(1,4),100),
      (Array(2,4),100),
      (Array(0,1),100),
      (Array(1,2),100),
      (Array(0,2),50)
    )

    val near5Clique = Array(
      (Array(1,4),8),
      (Array(2,4),8),
      (Array(1,3),8),
      (Array(0,3),4),
      (Array(2,3),8),
      (Array(0,1),8),
      (Array(1,2),4),
      (Array(0,2),8)
    )


    val hyberCubeOptimizer = new HyberCubeOptimizer(near5Clique,200,224,5)
    val possiblePlans = hyberCubeOptimizer.allPlans()

    val minCostPlan = possiblePlans.minBy(f => f._3.toDouble/f._1.product)
    val minCost = minCostPlan._3

    possiblePlans
      .filter(f => f._3 < 1.5*minCost)
      .sortBy(f => f._3.toDouble/f._1.product).reverse.foreach{
      f =>

        println()
        print(" P assignment:")
        f._1.foreach(h => print(s"$h "))

        print(" subtasks:"+f._1.product)

        print(s" Total Cost: ${f._3}")

        f._2.foreach{
          h =>
            print(" |pattern:")
            h._1.foreach(w => print(s"$w "))

            print(" size:")
            print(h._2)

            print(" totalSize:")
            print(h._3)

            print(" repetition:")
            print(h._4)
        }
    }


    println()
    print(" P assignment:")
    minCostPlan._1.foreach(h => print(s"$h "))

    print(" subtasks:"+minCostPlan._1.product)

    print(s" Total Cost: ${minCostPlan._3}")

    minCostPlan._2.foreach{
      h =>
        print(" |pattern:")
        h._1.foreach(w => print(s"$w "))

        print(" size:")
        print(h._2)

        print(" totalSize:")
        print(h._3)

        print(" repetition:")
        print(h._4)
    }

  }
}
