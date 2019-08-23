package org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.Decrapted

import org.apache.spark.adj.utlis.ListGenerator

class LazyOptimizer(val patternMap:Map[String,Int], val tree:QTree) {


  def optimize() = {

    val patterns = patternMap.keys.filter(p => p != "Edge" && p != "Communication" && p != "Computation" && p != "Machine").toIndexedSeq
    val options = ListGenerator.fillList(2,patterns.size)
    val lazyPlan = ListGenerator.cartersianSizeList(options)
      .map{f =>
      f.map { p => p match {
        case 0 => false
        case 1 => true
        }
       }
      }
      .map{
      f => patterns.zip(f).toList :+ ("Edge",true)
      }.map(f => f.toMap).zipWithIndex.toParArray


    val res = lazyPlan.map{
      f =>
        val lazyMap = f._1
        val index = f._2
        val hNumberDecider = new HNumberDecider(patternMap,lazyMap,tree,index)

        (f,hNumberDecider.invokeOctave())
//        hNumberDecider.costProgramming
    }.toList.sortBy(f => f._2._2)


    res.foreach{f =>
      println("plan:")
      println(f._1)
      f._2._1.foreach(p => print(" " + p))
      println()
      println(f._2._2)


    }

  }
}
