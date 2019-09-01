package leapfrog

import org.apache.spark.adj.execution.leapfrog.{ArraySegment, ArrayTrie}
import org.apache.spark.adj.utils.testing.TestingHelper
import org.scalatest.FunSuite

import scala.util.Random

class TrieTest extends FunSuite {

  test("trie -- relation restore") {
    val num = 100
    val arity = 4

    val table = Range(0, num).map { _ =>
      Range(0, arity).map { _ =>
        Math.abs(Random.nextInt() % (2 * num))
      }.toArray
    }.toArray

    println(s"input relation is:")
    table.foreach { t =>
      println(t.toSeq)
    }

    val trie = ArrayTrie(table, arity)

    println(s"trie is:")
    println(trie)

    val restoredRelation = trie.toRelation()
    println(s"restored relation is:")
    restoredRelation.foreach { t =>
      println(t.toSeq)
    }

    assert(restoredRelation.map(_.toSeq).diff(table.map(_.toSeq)).size == 0)
  }

  test("trie -- empty") {
    val num = 0
    val arity = 4

    val table = TestingHelper.genRandomContent(num, arity)
    val trie = ArrayTrie(table, arity)

    val restoredRelation = trie.toRelation()
    assert(restoredRelation.size == 0)

    val array = ArraySegment(Array(0, 1, 0))
    assert(trie.nextLevel(array).size == 0)
  }

  test("trie -- duplicated") {
    val num = 50
    val arity = 4

    var table = TestingHelper.genRandomContent(num, arity)
    table = table ++ table

    println(s"input relation is:")
    table.foreach { t =>
      println(t.toSeq)
    }

    val trie = ArrayTrie(table, arity)

    val restoredRelation = trie.toRelation()
    assert(restoredRelation.map(_.toSeq).diff(table.map(_.toSeq)).size == 0)
    assert(restoredRelation.size == table.size)

    println(s"restored relation is:")
    restoredRelation.foreach { t =>
      println(t.toSeq)
    }
  }

}
