package leapfrog

import org.apache.spark.adj.leapfrog.{Alg, Intersection}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class AlgTest extends FunSuite{

  test("leapfrog intersection -- boundary case"){
    val num = 100
    val array1 = Range(0,num).map(_ => Math.abs(Random.nextInt() % (100*num))).sorted.distinct.toArray
    val array2 = Range(0,10*num).map(_ => Math.abs(Random.nextInt() % (100*num))).sorted.distinct.toArray
    val array3 = Range(0,5*num).map(_ => Math.abs(Random.nextInt() % (100*num))).sorted.distinct.toArray

    val arrays1 = Array(array1, array2, Array[Int]())
    val arrays2 = Array(array1)

    assert(Alg.leapfrogIntersection(arrays1).isEmpty)
    assert(Alg.leapfrogIntersection(arrays2).toSeq.diff(arrays2(0).toSeq).isEmpty)
  }

  test("leapfrog intersection -- speed and correctness"){

    var mergelikeTimes = ArrayBuffer[Long]()
    var leapfrogTimes = ArrayBuffer[Long]()

    def testFunc() = {
      val num = 100
      val array1 = Range(0,num).map(_ => Math.abs(Random.nextInt() % (100*num))).sorted.distinct.toArray
      val array2 = Range(0,10*num).map(_ => Math.abs(Random.nextInt() % (100*num))).sorted.distinct.toArray
      val array3 = Range(0,5*num).map(_ => Math.abs(Random.nextInt() % (100*num))).sorted.distinct.toArray
      val array4 = Range(0,3*num).map(_ => Math.abs(Random.nextInt() % (100*num))).sorted.distinct.toArray

      val arrays = Array(array1, array2, array3, array4)

      val startTime = System.nanoTime()

      val out1 = Alg.mergelikeIntersection(arrays)
      val endTime1 = System.nanoTime()

      val out2 = Alg.leapfrogIntersection(arrays)
      val endTime2 = System.nanoTime()

      mergelikeTimes += (endTime1 - startTime) / 1000000
      leapfrogTimes += (endTime2 - endTime1) / 1000000

      if (out1.zip(out2).forall(x => x._1 == x._2) == false){
        println()
        println(s"mergelike results:")
        out1.foreach(x => print(s"${x};"))

        println(s"\nleapfrog results:")
        out2.foreach(x => print(s"${x};"))
        println()
      }
      assert(out1.zip(out2).forall(x => x._1 == x._2))
    }

    Range(0,10000).toParArray.foreach(_ => testFunc())

    println(s"mergeLike time:${mergelikeTimes.sum} ms, leapfrog time:${leapfrogTimes.sum} ms, winner is leapfrog? ${leapfrogTimes.sum < mergelikeTimes.sum}")
//    out2.foreach(x => print(s"${x};"))

  }
}
