package leapfrog

import org.apache.spark.adj.leapfrog.LeapFrog
import org.scalatest.FunSuite
import utils.TestingData

class LeapFrogTest extends FunSuite{


  test("prepare testing data"){
    val subjoins1 = TestingData.testing_subjoins1
    println(subjoins1)
    println(subjoins1.blocks.map(_.content.toSeq.map(_.toSeq)))
  }

  test("basic operation"){
    val subJoins1 = TestingData.testing_subjoins1
    val leapfrog = new LeapFrog(subJoins1)
    leapfrog.debugInternal()
  }




}
