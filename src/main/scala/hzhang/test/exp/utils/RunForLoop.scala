package hzhang.test.exp.utils

import org.apache.spark.adj.utils.SparkSingle

import scala.collection.mutable

class RunForLoop(times: Long) {



  def run() ={
    val sc = SparkSingle.getSparkContext()
    val count1 = times/224
    val rdd = sc.parallelize(Range(1,224)).repartition(224)

    rdd.mapPartitions{f =>
      var count = count1
      var zero = 145.0
      val theMap = new mutable.LongMap[Int]()
      var theNumber = 10000

      while(theNumber > 0){
        theMap += (theNumber, theNumber)
        theNumber -= 1
      }

      while(count > 0){
        zero = zero+theMap.getOrElse((zero % theNumber).toInt,0)
      }

      Iterator(zero)
    }.sum()
  }


}
