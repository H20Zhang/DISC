package leapfrog

import org.apache.spark.adj.leapfrog.LeapFrog
import org.scalatest.FunSuite
import utils.{TestingSubJoins, TestingHelper}

class LeapFrogTest extends FunSuite{


  test("prepare testing data"){

    val graphRelation = TestingHelper.genGraphContent("debugData")
    println(graphRelation.size)
    println(graphRelation.toSeq.map(_.toSeq))

//    val subjoins1 = TestingData.testing_subjoins2
//    println(subjoins1)
//    println(subjoins1.blocks.map(_.content.toSeq.map(_.toSeq)))
  }

  test("debug internal"){
    val subJoins1 = TestingSubJoins.testing_subjoins1
    val leapfrog = new LeapFrog(subJoins1)
    leapfrog.debugInternal()
  }

  test("leapfrog"){

    val subJoins = TestingSubJoins.testing_subjoins2
    val leapfrog = new LeapFrog(subJoins)
//    leapfrog.init()


//    leapfrog.tries.foreach(trie => println(trie.toRelation().toSeq.map(_.toSeq)))
    println(leapfrog.tries.size)
    leapfrog.relevantRelationForAttrMap.toSeq.map(f => f.map(x => (x._1, x._2, x._3.toSeq, x._4))).foreach(println)

    println(leapfrog.size)
//    while(leapfrog.hasNext){
//      println(leapfrog.next())
//    }


//    println(leapfrog.toSeq)
  }




}
