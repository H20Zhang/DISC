package Plan

import org.apache.spark.Logo.Plan.LogicalPlan.CostBasedOptimizer._
import org.scalatest.FunSuite

class CostBasedOptimizerTest extends FunSuite{

  val core = 224
  val p = 11
  val edges = 13
  val triangleSize = 387
  val squareSize = 1126460
  val triangleSquareQueryTime = 19000892
  val squareTriangleQueryTime = 4386
  val squareSquareQueryTime = 9000892 * edges
  val triangleTriangleQueryTime = 15062 * edges
  val memLimit = Math.pow(10,2)
  val commSpeed = 2
  val offsetFix = 2 * 1000 //compensate for ms and symmetry breaking
  val qCost = QueryCost(Map(((0,1),triangleSquareQueryTime),((1,0),squareTriangleQueryTime), ((0,0),triangleTriangleQueryTime),((1,1),squareSquareQueryTime)))

  val subPattern0 = new SubPattern(0,
    SubPatternStructure(Seq(0,1,4),Seq()),
    p*p*edges,
    triangleSize)

  val subPattern1 = new SubPattern(1,
    SubPatternStructure(Seq(1,2,3,4),Seq()),
    p*p*edges,
    squareSize)

  val optimizer = CostBaseOptimizer(Seq(subPattern0,subPattern1), qCost, commSpeed, memLimit, core, offsetFix)

  test("subPattern"){
    val orderedSubPattern0 = OrderedSubPattern(subPattern0,0,false)
    println(orderedSubPattern0.commTime(2))

    val orderedSubPattern1 = OrderedSubPattern(subPattern1,0,false)
    println(orderedSubPattern1.commTime(2))

    println(orderedSubPattern0.compTime(orderedSubPattern1,qCost,memLimit,core,offsetFix,2))
  }

  test("all"){
    //weB,


    optimizer.displayAllPlan




  }

}
