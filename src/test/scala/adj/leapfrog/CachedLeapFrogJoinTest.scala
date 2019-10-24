package adj.leapfrog

import org.apache.spark.adj.database.Catalog.DataType
import org.apache.spark.adj.execution.subtask.executor.IntArrayLRUCache
import org.apache.spark.adj.execution.subtask.utils.ArraySegment
import org.scalatest.FunSuite

import scala.collection.mutable

class CachedLeapFrogJoinTest extends FunSuite {

  test("LRUCache") {
    val cache = new IntArrayLRUCache(10)
    Range(0, 20).foreach { idx =>
      val array = new Array[Int](1)
      val key = mutable.WrappedArray.make[Int](array)
      key(0) = idx
      println(idx)
      cache(key) = Range(0, idx).toArray.map(f => Array(f))
    }

//    println(cache)

    val array = new Array[Int](1)
    val key = mutable.WrappedArray.make[Int](array)
    Range(5, 15).foreach { idx =>
      key(0) = idx
      if (cache.get(key) != null) {
        println(cache(key).toSeq)
      } else {
        println(idx)
      }

    }

  }
}
