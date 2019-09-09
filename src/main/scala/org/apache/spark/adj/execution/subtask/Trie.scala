package org.apache.spark.adj.execution.subtask

import java.util.Comparator

import org.apache.spark.adj.database.Catalog.DataType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait Trie extends Serializable {
  def nextLevel(binding: ArraySegment): ArraySegment
  def toRelation(): Array[Array[DataType]]
}

// edge:Array[(ID, ID, Value)], node:Array[(Start, End)]
class ArrayTrie(neighbors: Array[Int],
                values: Array[Int],
                neighborBegins: Array[Int],
                neighborEnds: Array[Int],
                level: Int)
    extends Trie {

  var rootBegin = neighborBegins(0)
  var rootEnd = neighborEnds(0)
//  val rootLevelMap = mutable.HashMap[Int, Int]() ++ values
//    .slice(rootBegin, rootEnd)
//    .zip(Range(rootBegin, rootEnd))

  def nextLevel(binding: ArraySegment): ArraySegment = {

    var id = 0
    var start = rootBegin
    var end = rootEnd

    val level = binding.size
    var i = 0

    while (i < level) {
      val pos = BSearch.search(values, binding(i), start, end)

      if (pos == -1) {
        return ArraySegment.emptyArray()
      }

      id = neighbors(pos)
      start = neighborBegins(id)
      end = neighborEnds(id)
      i += 1
    }

    ArraySegment(values, start, end, end - start)
  }

  //just for verify the correctness of the trie implementation
  def toRelation(): Array[Array[DataType]] = {
    var tables =
      nextLevel(ArraySegment.emptyArray()).toArray().map(f => Array(f))

    var i = 1
    while (i < level) {
      tables = tables.flatMap { f =>
        val nextLevelValues = nextLevel(ArraySegment(f))
        nextLevelValues.toArray().map(value => f :+ value)
      }

      i += 1
    }

    tables
  }

  override def toString: String = {
    s"""
       |neighbors:${neighbors.toSeq}
       |values:${values.toSeq}
       |neighborStart:${neighborBegins.toSeq}
       |neighborEnd:${neighborEnds.toSeq}
     """.stripMargin
  }

}

//TODO: debug
class HashMapTrie(rootLevel: ArraySegment,
                  nextLevelMap: mutable.HashMap[Int, mutable.HashMap[Array[
                    DataType
                  ], ArraySegment]],
                  arity: Int)
    extends Trie {

  val tempArrayForIthLevel = mutable.HashMap[Int, Array[DataType]]()

  init()
  def init() = {
    Range(1, arity).foreach { i =>
      tempArrayForIthLevel(i) = new Array[DataType](i)
    }
  }

  override def nextLevel(binding: ArraySegment): ArraySegment = {
    val bindingSize = binding.size
    val level = bindingSize - 1

    if (bindingSize == 0) {
      return rootLevel
    } else {
      val tempArray = tempArrayForIthLevel(level)
      var i = 0
      while (i < bindingSize) {
        tempArray(i) = binding(i)
      }

      return nextLevelMap(level)(tempArray)
    }
  }
  override def toRelation(): Array[Array[DataType]] = ???
}

object HashMapTrie {
  def apply(table: Array[Array[DataType]], arity: Int): HashMapTrie = {
    val rootLevel = ArraySegment(table.map(t => t(0)).distinct)
    val nextLevelMap =
      mutable.HashMap[Int, mutable.HashMap[Array[DataType], ArraySegment]]()
    var i = 0
    while (i < arity - 1) {
      var projectedTable = ArrayBuffer[Array[DataType]]()
      table.foreach { tuple =>
        val projectedTuple = new Array[DataType](i + 1)
        var j = 0
        while (j <= i) {
          projectedTuple(j) = tuple(j)
        }

        projectedTable += projectedTuple
      }

      projectedTable = projectedTable.distinct

      val levelMap = mutable.HashMap[Array[DataType], ArrayBuffer[DataType]]()

      projectedTable.foreach { tuple =>
        val key = new Array[DataType](i)
        var j = 0
        while (j < i) {
          key(i) = tuple(i)
        }

        levelMap.get(key) match {
          case Some(buffer) => buffer += tuple(i)
          case None =>
            val buffer = ArrayBuffer(tuple(i)); levelMap(key) = buffer
        }
      }

      nextLevelMap(i) =
        levelMap.map(f => (f._1, ArraySegment(f._2.toArray.sorted)))
    }

    new HashMapTrie(rootLevel, nextLevelMap, arity)
  }
}

// Scan the tuples of the relation sequentially, for each tuple,
// locate the bit where it firstly diverge from the previous tuple and then create a new trie node.
object ArrayTrie {
  def apply(table: Array[Array[DataType]], arity: Int): ArrayTrie = {

    //sort the relation in lexical order
    val comparator = new LexicalOrderComparator(arity)
    java.util.Arrays.sort(table, comparator)

    //init
    var idCounter = 0
    var leafIDCounter = -1
    val prevIDs = new Array[Int](arity)
    var prevTuple = new Array[Int](arity)
    val edgeBuffer = ArrayBuffer[(Int, Int, DataType)]()
    val nodeBuffer = ArrayBuffer[(Int, Int)]()
    val tableSize = table.size

    var i = 0
    while (i < arity) {
      prevIDs(i) = 0
      prevTuple(i) = Int.MaxValue
      i += 1
    }

    //construct edge for ArrayTrie
    val rootID = idCounter
    idCounter += 1

    i = 0
    while (i < tableSize) {
      val curTuple = table(i)

      //find the j-th position where value of curTuple diverge from prevTuple
      var diffPos = -1
      var isConsecutive = true
      var j = 0
      while (j < arity) {
        if (curTuple(j) != prevTuple(j) && isConsecutive == true) {
          diffPos = j
          isConsecutive = false
        }
        j += 1
      }

      //deal with the case, where curTuple is the as prevTuple
      if (isConsecutive) {
        diffPos = arity - 2
      } else {
        diffPos = diffPos - 1
      }

      //for each value of curTuple diverge from preTuple, create a new NodeID
      //create edge between NodeIDs
      while (diffPos < arity - 1) {
        val nextPos = diffPos + 1
        var prevID = 0

        if (diffPos == -1) {
          prevID = rootID
        } else {
          prevID = prevIDs(diffPos)
        }

        var newID = 0

        if (diffPos < arity - 2) {
          newID = idCounter
          idCounter += 1
        } else {
          newID = leafIDCounter
          leafIDCounter -= 1
        }

        //create edge between NodeIDs
        edgeBuffer += ((prevID, newID, curTuple(nextPos)))
        prevIDs(nextPos) = newID

        diffPos += 1
      }

      prevTuple = curTuple
      i += 1
    }

    //add a tuple to mark the end of the edge
    edgeBuffer += ((Int.MaxValue, Int.MaxValue, Int.MaxValue))

    //sort edge first by "id" then "value"
    val edge = edgeBuffer.toArray
    val edgeComparator = new EdgeComparator
    java.util.Arrays.sort(edge, edgeComparator)

    //construct node for ArrayTrie
    //  scan the sorted edge
    i = 0
    var start = 0
    var end = 0
    var currentValue = 0

    while (i < edge.size) {
      if (edge(i)._1 == currentValue) {
        end += 1
      } else {
        nodeBuffer += ((start, end))

        currentValue = edge(i)._1
        start = i
        end = i + 1
      }
      i += 1
    }

    new ArrayTrie(
      edge.dropRight(1).map(_._2),
      edge.dropRight(1).map(_._3),
      nodeBuffer.map(_._1).toArray,
      nodeBuffer.map(_._2).toArray,
      arity
    )
  }
}

class LexicalOrderComparator(attrNum: Int)
    extends Comparator[Array[DataType]]
    with Serializable {

  override def compare(o1: Array[DataType], o2: Array[DataType]): Int = {
    var i = 0
    while (i < attrNum) {
      if (o1(i) < o2(i)) {
        return -1
      } else if (o1(i) > o2(i)) {
        return 1
      } else {
        i += 1
      }
    }
    return 0
  }
}

class EdgeComparator
    extends Comparator[(Int, Int, DataType)]
    with Serializable {

  override def compare(o1: (Int, Int, DataType),
                       o2: (Int, Int, DataType)): Int = {

    if (o1._1 < o2._1) {
      return -1
    } else if (o1._1 > o2._1) {
      return 1
    } else {
      if (o1._3 < o2._3) {
        return -1
      } else if (o1._3 > o2._3) {
        return 1
      } else return 0
    }
  }
}
