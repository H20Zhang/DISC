package adj.leapfrog

import org.apache.spark.adj.execution.subtask.utils.{Alg, ArraySegment}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class AlgTest extends FunSuite {

  test("adj.leapfrog intersection -- boundary case") {
    val num = 100
    val array1 = Range(0, num)
      .map(_ => Math.abs(Random.nextInt() % (100 * num)))
      .sorted
      .distinct
      .toArray
    val array2 = Range(0, 10 * num)
      .map(_ => Math.abs(Random.nextInt() % (100 * num)))
      .sorted
      .distinct
      .toArray
    val array3 = Range(0, 5 * num)
      .map(_ => Math.abs(Random.nextInt() % (100 * num)))
      .sorted
      .distinct
      .toArray

    val arrays1 = Array(array1, array2, Array[Int]())
    val arrays2 = Array(array1)

    assert(Alg.leapfrogIntersection(arrays1).isEmpty)
    assert(
      Alg.leapfrogIntersection(arrays2).toSeq.diff(arrays2(0).toSeq).isEmpty
    )
  }

  test("adj.leapfrog intersection -- speed and correctness") {

    var mergelikeTimes = ArrayBuffer[Long]()
    var leapfrogTimes = ArrayBuffer[Long]()

    def testFunc() = {
      val num = 100
      val array1 = Range(0, num)
        .map(_ => Math.abs(Random.nextInt() % (100 * num)))
        .sorted
        .distinct
        .toArray
      val array2 = Range(0, 10 * num)
        .map(_ => Math.abs(Random.nextInt() % (100 * num)))
        .sorted
        .distinct
        .toArray
      val array3 = Range(0, 5 * num)
        .map(_ => Math.abs(Random.nextInt() % (100 * num)))
        .sorted
        .distinct
        .toArray
      val array4 = Range(0, 3 * num)
        .map(_ => Math.abs(Random.nextInt() % (100 * num)))
        .sorted
        .distinct
        .toArray

      val arrays = Array(array1, array2, array3, array4)

      val startTime = System.nanoTime()

      val out1 = Alg.mergelikeIntersection(arrays)
      val endTime1 = System.nanoTime()

      val out2 = Alg.leapfrogIntersection(arrays)
      val endTime2 = System.nanoTime()

      mergelikeTimes += (endTime1 - startTime) / 1000000
      leapfrogTimes += (endTime2 - endTime1) / 1000000

      if (out1.zip(out2).forall(x => x._1 == x._2) == false) {
        println()
        println(s"mergelike results:")
        out1.foreach(x => print(s"${x};"))

        println(s"\nadj.leapfrog results:")
        out2.foreach(x => print(s"${x};"))
        println()
      }
      assert(out1.zip(out2).forall(x => x._1 == x._2))
    }

    Range(0, 10000).toParArray.foreach(_ => testFunc())

    println(
      s"mergeLike time:${mergelikeTimes.sum} ms, adj.leapfrog time:${leapfrogTimes.sum} ms, winner is adj.leapfrog? ${leapfrogTimes.sum < mergelikeTimes.sum}"
    )
//    out2.foreach(x => print(s"${x};"))
  }

  test("adj.leapfrog iterator") {
    var listItTimes = ArrayBuffer[Long]()
    var leapfrogItTimes = ArrayBuffer[Long]()

    def testFunc() = {
      val num = 10000
      val array1 = ArraySegment(
        Range(0, num)
          .map(_ => Math.abs(Random.nextInt() % (2 * num)))
          .sorted
          .distinct
          .toArray
      )
      val array2 = ArraySegment(
        Range(0, num)
          .map(_ => Math.abs(Random.nextInt() % (2 * num)))
          .sorted
          .distinct
          .toArray
      )
      val array3 = ArraySegment(
        Range(0, num)
          .map(_ => Math.abs(Random.nextInt() % (2 * num)))
          .sorted
          .distinct
          .toArray
      )
      val array4 = ArraySegment(
        Range(0, num)
          .map(_ => Math.abs(Random.nextInt() % (2 * num)))
          .sorted
          .distinct
          .toArray
      )

      val arrays = Array(array1, array2, array3, array4)

      val startTime = System.nanoTime()

      val out1 = Alg.listIt(arrays)
      val out1Array = out1.toArray.toSeq
      val endTime1 = System.nanoTime()

      val out2 = Alg.leapfrogIt(arrays)
      val out2Array = out2.toArray.toSeq
      val endTime2 = System.nanoTime()

      listItTimes += (endTime1 - startTime) / 1000000
      leapfrogItTimes += (endTime2 - endTime1) / 1000000

      assert(out1Array.size == out2Array.size)

      if (out1Array.zip(out2Array).forall(x => x._1 == x._2) == false) {
        println()
        println(s"mergelike results:")
        out1.foreach(x => print(s"${x};"))

        println(s"\nadj.leapfrog results:")
        out2.foreach(x => print(s"${x};"))
        println()
      }
      assert(out1Array.zip(out2Array).forall(x => x._1 == x._2))
    }

    Range(0, 1000).toParArray.foreach(_ => testFunc())

    println(
      s"listIt time:${listItTimes.sum} ms, leapfrogIt time:${leapfrogItTimes.sum} ms, winner is adj.leapfrog? ${leapfrogItTimes.sum < listItTimes.sum}"
    )
  }

}
