package org.apache.spark.Logo.Plan.LogicalPlan.CostBasedOptimizer

import org.apache.spark.Logo.UnderLying.utlis.Experiment.ExamplePatternSampler
import org.apache.spark.Logo.UnderLying.utlis.ListGenerator


//Below optimizer only work for cases where there are only two sub-patterns, for more sub-patterns, some of
//the belows codes need to be changed.
//commSpeed:GB/s, cardinality: 10^6
case class CostBaseOptimizer(subPatterns:Seq[SubPattern], qCost:QueryCost, commSpeed:Double, memLimit:Double, core:Int, offsetFix:Double) {

  def generateOrderedSubPatterns:Seq[Seq[OrderedSubPattern]] = {
    val idList = subPatterns.map(_.id)
    val orderedIdLists = idList.permutations.toSeq
    val laziedOrderedIdLists = orderedIdLists.flatMap{f =>
      val isLazyList = ListGenerator.cartersianSizeList(ListGenerator.fillList(2,idList.size))
      isLazyList.map(g => g.map(h => h == 1).zip(f))
    }
    laziedOrderedIdLists.map(f => f.zip(subPatterns).map(g => OrderedSubPattern(g._2,g._1._2,g._1._1)))
  }

  def calculateCosts(plan: Seq[OrderedSubPattern]):(Seq[OrderedSubPattern],Double) = {
    val commCosts = plan.map(_.commTime(commSpeed)).sum
    var compCosts = 0.0
    val orderedPlan = plan.sortBy(_.order)
    compCosts = orderedPlan.drop(1).foldLeft(compCosts){(a,b) =>
      val prevSubPattern = orderedPlan(b.order-1)
      a + b.compTime(prevSubPattern,qCost,memLimit,core, offsetFix, commSpeed)
    }

    (plan,commCosts*0.5+compCosts)
  }

  def displayAllPlan = {
    val plansWithCost = generateOrderedSubPatterns.map(f => calculateCosts(f)).sortBy(_._2)

    plansWithCost.map(f => (f._1.map(g => (g.id,g.order,g.isLazy)),f._2)).foreach(println)
  }

  def retriveAllResults = {
    val plansWithCost = generateOrderedSubPatterns.map(f => calculateCosts(f)).sortBy(_._2)

    plansWithCost.map(f => (f._1.map(g => (g.id,g.order,g.isLazy)),f._2))
  }

}

class HouseCostEstimator(data:String, h:Int, k:Int, k2:Int){


  val query = List("triangle", "square", "squareTriangle", "triangleSquare")

  lazy val informMaps = generateInformations()
  var rawEdgeSize1 = 0L


  def generateInformations() ={
    val base = k
    println(s"k is ${k}")
    val sampledPatterns = new ExamplePatternSampler(data,h,h,k, k2)
    val rawEdgeSize = sampledPatterns.rawEdgeSize
    rawEdgeSize1 = rawEdgeSize
    val sampledRawEdgeSize = sampledPatterns.sampledRawEdgeSize
    val ratio = sampledRawEdgeSize.toDouble / rawEdgeSize


    val informMaps = query.map(f => (f,f)).map{
      f =>
        val sampledPattern = sampledPatterns.pattern(f._2)
        val time_size_pair = sampledPattern.time_size()

        //amount of object sampled
        val sampledSize = (time_size_pair._1 / ratio).toLong

        //size, time
        (f._1,(sampledSize/base,time_size_pair._2))
    }.toMap

    informMaps
  }

  def generatePlans(memLimit:Double) = {

    //i must be 10^6
    val commSpeed = 2
    val offsetFix = 2 * 1000 //compensate for ms and symmetry breaking
    val core = 224
    val p = 11
    val edges = rawEdgeSize1 /  Math.pow(10,6)
    val triangleSize = informMaps("triangle")._1
    val squareSize = informMaps("square")._1
    val triangleSquareQueryTime = informMaps("triangleSquare")._2
    val squareTriangleQueryTime = informMaps("squareTriangle")._2
    val squareSquareQueryTime = informMaps("square")._2 * edges
    val triangleTriangleQueryTime = informMaps("triangle")._2 * edges
    //    val memLimit = Math.pow(10,2)

    val qCost = QueryCost(Map(((0,1),triangleSquareQueryTime),((1,0),squareTriangleQueryTime), ((0,0),triangleTriangleQueryTime),((1,1),squareSquareQueryTime)))

    val subPattern0 = new SubPattern(0,
      SubPatternStructure(Seq(0,1,4),Seq()),
      p*p*2*edges,
      triangleSize)

    val subPattern1 = new SubPattern(1,
      SubPatternStructure(Seq(1,2,3,4),Seq()),
      p*p*2*edges,
      squareSize)

    val optimizer = CostBaseOptimizer(Seq(subPattern0,subPattern1), qCost, commSpeed, memLimit, core, offsetFix)

    val res = optimizer.retriveAllResults

    val nameMap = Map((0,"triangle"), (1,"square"))
    val lazyMap = Map((true, "lazy"), (false, "eager"))

    res.map(f => (f._1.sortBy(g => g._2).map(g => (nameMap(g._1),lazyMap(g._3))), f._2))
  }


}

class near5CliqueCostEstimator(data:String, h:Int, k:Int, k2:Int){


  val query = List("triangle", "fourClique", "fourCliqueTriangle", "triangleFourClique")
  var rawEdgeSize1 = 0L

  lazy val informMaps = generateInformations()

  def generateInformations() ={
    val base = k
    println(s"k is ${k}")
    val sampledPatterns = new ExamplePatternSampler(data,h,h,k)
    val rawEdgeSize = sampledPatterns.rawEdgeSize
    rawEdgeSize1 = rawEdgeSize
    val sampledRawEdgeSize = sampledPatterns.sampledRawEdgeSize
    val ratio = sampledRawEdgeSize.toDouble / rawEdgeSize

    query.map{
      f =>
        val sampledPattern = sampledPatterns.pattern(f)
        val start_time = System.currentTimeMillis()
        val time_size_pair = sampledPattern.time_size()
        val sampledSize = (time_size_pair._1 / ratio).toLong

        val end_time = System.currentTimeMillis()

        (f,sampledSize,time_size_pair._2,end_time-start_time, time_size_pair._1)
    }.foreach(f => println(s"${f._1}:sampledSize:${f._5/base}:estimatedSize:${f._2/base}:sampleTime:${f._3} in one Block :timeUsed:${f._4}"))

    val informMaps = query.map(f => (f,f)).map{
      f =>
        val sampledPattern = sampledPatterns.pattern(f._2)
        //        val start_time = System.currentTimeMillis()
        val time_size_pair = sampledPattern.time_size()

        //amount of object sampled
        val sampledSize = (time_size_pair._1 / ratio).toLong

        //        val end_time = System.currentTimeMillis()

        //size, time
        (f._1,(sampledSize/base,time_size_pair._2))
    }.toMap

    informMaps
  }

  def generatePlans(memLimit:Double) = {

    //i must be 10^6

    val commSpeed = 2
    val offsetFix = 2 * 1000 //compensate for ms and symmetry breaking
    val core = 224
    val p = 11
    val edges = rawEdgeSize1 / Math.pow(10,6)
    val triangleSize = informMaps("triangle")._1
    val fourCliqueSize = informMaps("fourClique")._1
    val triangleFourCliqueQueryTime = informMaps("triangleFourClique")._2
    val squareFourCliqueQueryTime = informMaps("fourCliqueTriangle")._2
    val fourCliqueFourCliqueQueryTime = informMaps("fourClique")._2 * edges
    val triangleTriangleQueryTime = informMaps("triangle")._2 * edges

    val qCost = QueryCost(Map(((0,1),triangleFourCliqueQueryTime),((1,0),squareFourCliqueQueryTime), ((0,0),triangleTriangleQueryTime),((1,1),fourCliqueFourCliqueQueryTime)))

    val subPattern0 = new SubPattern(0,
      SubPatternStructure(Seq(0,1,4),Seq()),
      p*p*2*edges,
      triangleSize)

    val subPattern1 = new SubPattern(1,
      SubPatternStructure(Seq(1,2,3,4),Seq()),
      p*p*3*edges,
      fourCliqueSize)

    val optimizer = CostBaseOptimizer(Seq(subPattern0,subPattern1), qCost, commSpeed, memLimit, core, offsetFix)

    val res = optimizer.retriveAllResults

    val nameMap = Map((0,"triangle"), (1,"fourClique"))
    val lazyMap = Map((true, "lazy"), (false, "eager"))

    res.map(f => (f._1.sortBy(g => g._2).map(g => (nameMap(g._1),lazyMap(g._3))), f._2))
  }


}
