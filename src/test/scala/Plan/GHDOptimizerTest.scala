package Plan

import org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize.SubSetsGenerator
import org.apache.spark.Logo.UnderLying.utlis.TestUtil
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class GHDOptimizerTest extends FunSuite{


  test("subsetGenerator"){

    val generator = new SubSetsGenerator(ArrayBuffer(0,1,2,3,4,5,6,7,8,9))

    val subsets = generator.enumerateSet()

//    subsets.foreach{f =>
//      println()
//      f.foreach{w =>
//        print("|")
//        w.foreach(u => print(s"$u "))}
//    }

    println(subsets.size)

    val x = 1


  }


}
