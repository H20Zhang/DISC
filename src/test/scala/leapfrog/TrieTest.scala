package leapfrog

import org.apache.spark.adj.leapfrog.{ArraySegment, ArrayTrie}
import org.scalatest.FunSuite

import scala.util.Random

class TrieTest extends FunSuite{

  test("trie"){
    val num = 10
    val arity = 4

    val table = Range(0,num).map{
      _ =>
        Range(0, arity).map{
          _ => Math.abs(Random.nextInt() % (2*num))
        }.toArray
    }.toArray

    println(s"input relation is:")
    table.foreach{t =>
      println(t.toSeq)
    }

//    val attrs = Range(0,arity).toArray
    val trie = ArrayTrie(table, arity)

    println(s"trie is:")
    println(trie)

    val restoredRelation = trie.toRelation()

    assert(restoredRelation.map(_.toSeq).diff(table.map(_.toSeq)).size == 0)

    println(s"restored relation is:")
    restoredRelation.foreach{t =>
      println(t.toSeq)
    }

  }
}
