package LogicalPlan

import org.apache.spark.Logo.Plan.LogicalPlan.Decrapted.HyberCubeOptimize.{HyberCubeOptimizer, PGenerator}
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


    //triangle
    val triangle = Array(
      (Array(0,1),2),
      (Array(0,2),2),
      (Array(1,2),2)
    )

    //square
    val square = Array(
      (Array(0,1),2),
      (Array(1,2),2),
      (Array(2,3),2),
      (Array(3,0),2)
    )

    //chordalSquare
    val chordalSquare = Array(
      (Array(0,1,2),2),
      (Array(0,2,3),2)
    )

    //fourClique
    val fourClique = Array(
      (Array(0,1),2),
      (Array(1,2),2),
      (Array(2,3),2),
      (Array(3,0),2),
      (Array(0,2),2),
      (Array(1,3),2)
    )

    //house
    val house = Array(
      (Array(0,1,2),6),
      (Array(2,3),2),
      (Array(3,4),2),
      (Array(4,0),2),
      (Array(0,2),2)
    )

    val threeTriangle = Array(
      (Array(1,3),100),
      (Array(0,3),100),
      (Array(1,4),100),
      (Array(2,4),100),
      (Array(0,1),100),
      (Array(1,2),100),
      (Array(0,2),100)
    )

    val fourTriangle = Array(
      (Array(1,3),100),
      (Array(0,3),100),
      (Array(1,4),100),
      (Array(2,4),100),
      (Array(0,1),100),
      (Array(1,2),100),
      (Array(0,2),100),
      (Array(1,5),100),
      (Array(3,5),100)
    )

    val near5Clique = Array(
      (Array(0,1,2),6),
      (Array(2,3),2),
      (Array(3,4),2),
      (Array(4,0),2),
      (Array(0,2),2),
      (Array(0,3),2),
      (Array(2,4),2)
    )




    val hyberCubeOptimizer = new HyberCubeOptimizer(square,224*2,224*4,4)
    val possiblePlans = hyberCubeOptimizer.detailAllPlans()

    val minCostPlan = possiblePlans.minBy(f => f._3.toDouble)
    val minCost = minCostPlan._3

    possiblePlans
      .filter(f => f._3 < 1.5*minCost)
      .sortBy(f => f._3.toDouble).reverse.foreach{
      f =>

        println()
        print(" P assignment:")
        f._1.foreach(h => print(s"$h "))

        print(" subtasks:"+f._1.product)

        print(s" Total Cost: ${f._3.toDouble}")

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
