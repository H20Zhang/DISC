package org.apache.spark.adj.execution.subtask.utils

import java.util.Comparator

import org.apache.spark.adj.database.Catalog.DataType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait Trie extends Serializable {
  def nextLevel(binding: ArraySegment): ArraySegment
  def toRelation(): Array[Array[DataType]]
}

object Trie {
  def apply(table: Array[Array[DataType]], arity: Int): Trie = {
//    if (arity == 2) {
//      GraphTrie(table, arity)
//    } else {
    ArrayTrie(table, arity)
//    }
  }
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
  val emptyArray = ArraySegment.emptyArraySegment

  //TODO: test whether replace some level by hashmap improves the speed
//  val rootLevelMap = {
//    val tempMap = mutable.LongMap[Int]()
//    var i = rootBegin
//    while (i < rootEnd) {
//      tempMap(values(i)) = i
//      i += 1
//    }
//    tempMap
//  }

  def nextLevel(binding: ArraySegment): ArraySegment = {

    var id = 0
    var start = rootBegin
    var end = rootEnd

    val level = binding.size
    var i = 0

    while (i < level) {

      var pos = 0
//      if (i == 100) {
//        val ithBinding = binding(i)
//        if (rootLevelMap.contains(ithBinding)) {
//          pos = rootLevelMap(ithBinding)
//        } else {
//          return emptyArray
//        }
//
//      } else {
      pos = BSearch.search(values, binding(i), start, end)
      if (pos == -1) {
        return emptyArray
      }
//      }

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

class GraphTrie(graph: mutable.HashMap[DataType, ArraySegment]) extends Trie {

  val rootLevel = ArraySegment(graph.keys.toArray.sorted)
  val emptyArray = ArraySegment.emptyArraySegment

  def nextLevel(binding: ArraySegment): ArraySegment = {
    val level = binding.size

    if (level == 0) {
      rootLevel
    } else {
      val bind = binding(0)
      if (graph.contains(bind)) {
        graph(bind)
      } else {
        emptyArray
      }
    }
  }

  override def toRelation(): Array[Array[DataType]] = {
    graph.toArray.flatMap(f => f._2.array.map(g => Array(f._1, g)))
  }
}

object GraphTrie {
  def apply(table: Array[Array[DataType]], arity: Int): GraphTrie = {
    assert(arity == 2)

    val graphBuffer = mutable.HashMap[DataType, ArrayBuffer[DataType]]()

    table.foreach { tuple =>
      val key = tuple(0)
      val value = tuple(1)
      if (graphBuffer.contains(key)) {
        graphBuffer(key) += value
      } else {
        val buffer = ArrayBuffer(value)
        graphBuffer(key) = buffer
      }
    }

    val graph = mutable.HashMap[DataType, ArraySegment]()

    graphBuffer.foreach {
      case (key, values) =>
        graph(key) = ArraySegment(values.toArray.sorted)
    }

    new GraphTrie(graph)

  }
}

class HashMapTrie(
  rootLevel: ArraySegment,
  nextLevelMap: mutable.HashMap[Int, mutable.HashMap[mutable.ArraySeq[DataType],
                                                     ArraySegment]],
  arity: Int
) extends Trie {

  val tempArrayForIthLevel = new Array[mutable.ArraySeq[DataType]](arity)

  init()
  def init() = {
    Range(0, arity).foreach { i =>
      tempArrayForIthLevel(i) = new mutable.ArraySeq[DataType](i + 1)
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
        i += 1
      }

      return nextLevelMap(level)(tempArray)
    }
  }
  override def toRelation(): Array[Array[DataType]] = ???
}

object HashMapTrie {
  def apply(table: Array[Array[DataType]], arity: Int): HashMapTrie = {
    val rootLevel = ArraySegment(table.map(t => t(0)).distinct.sorted)
    val nextLevelMap =
      mutable.HashMap[Int, mutable.HashMap[mutable.ArraySeq[DataType],
                                           ArraySegment]]()
    var keyPos = 0
    while (keyPos < arity - 1) {
      val valuePos = keyPos + 1
      val tupleSize = valuePos + 1
      var projectedTable = ArrayBuffer[Array[DataType]]()
      table.foreach { tuple =>
        val projectedTuple = new Array[DataType](tupleSize)
        var j = 0
        while (j <= keyPos + 1) {
          projectedTuple(j) = tuple(j)
          j += 1
        }

        projectedTable += projectedTuple
      }

      projectedTable = projectedTable.distinct

      val levelMap =
        mutable.HashMap[mutable.ArraySeq[DataType], ArrayBuffer[DataType]]()

      projectedTable.foreach { tuple =>
        val key = new mutable.ArraySeq[DataType](keyPos + 1)
        var j = 0
        while (j <= keyPos) {
          key(keyPos) = tuple(keyPos)
          j += 1
        }

        levelMap.get(key) match {
          case Some(buffer) => buffer += tuple(valuePos)
          case None =>
            val buffer = ArrayBuffer(tuple(valuePos)); levelMap(key) = buffer
        }
      }

      nextLevelMap(keyPos) =
        levelMap.map(f => (f._1, ArraySegment(f._2.toArray.sorted)))

      keyPos += 1
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
//    val edgeBuffer = ArrayBuffer[(Int, Int, DataType)]()
    val edgeBuffer = ArrayBuffer[ValuedEdge]()
    val nodeBuffer = ArrayBuffer[(Int, Int)]()
    val tableSize = table.size

    var i = 0
    while (i < arity) {
      prevIDs(i) = 0
      prevTuple(i) = Int.MaxValue
      i += 1
    }

    //construct edges for ArrayTrie
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
      //create edges between NodeIDs
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

        //create edges between NodeIDs
//        edgeBuffer += ((prevID, newID, curTuple(nextPos)))
        edgeBuffer += ValuedEdge(prevID, newID, curTuple(nextPos))
        prevIDs(nextPos) = newID

        diffPos += 1
      }

      prevTuple = curTuple
      i += 1
    }

    //add a tuple to mark the end of the edges
//    edgeBuffer += ((Int.MaxValue, Int.MaxValue, Int.MaxValue))
    edgeBuffer += ValuedEdge(Int.MaxValue, Int.MaxValue, Int.MaxValue)

    //sort edges first by "id" then "value"
    val edges = edgeBuffer.toArray
    val edgeComparator = new ValuedEdgeComparator
    java.util.Arrays.sort(edges, edgeComparator)

    //construct node for ArrayTrie
    //  scan the sorted edges
    i = 0
    var start = 0
    var end = 0
    var currentValue = 0

    while (i < edges.size) {
      if (edges(i).u == currentValue) {
        end += 1
      } else {
        nodeBuffer += ((start, end))

        currentValue = edges(i).u
        start = i
        end = i + 1
      }
      i += 1
    }

    val neighbors = new Array[DataType](edges.size - 1)
    val values = new Array[DataType](edges.size - 1)
    val neighborsBegin = new Array[DataType](nodeBuffer.size)
    val neighborsEnd = new Array[DataType](nodeBuffer.size)

    val edgeSize = edges.size - 1

    i = 0
    while (i < edges.size - 1) {
      val edge = edges(i)
      neighbors(i) = edge.v
      values(i) = edge.value
      i += 1
    }

    i = 0
    val nodesSize = nodeBuffer.size
    while (i < nodesSize) {
      val node = nodeBuffer(i)
      neighborsBegin(i) = node._1
      neighborsEnd(i) = node._2
      i += 1
    }

    new ArrayTrie(neighbors, values, neighborsBegin, neighborsEnd, arity)

//    new ArrayTrie(
//      edges.dropRight(1).map(_._2),
//      edges.dropRight(1).map(_._3),
//      nodeBuffer.map(_._1).toArray,
//      nodeBuffer.map(_._2).toArray,
//      arity
//    )
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

class TupleEdgeComparator
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

case class ValuedEdge(u: Int, v: Int, value: DataType)

class ValuedEdgeComparator extends Comparator[ValuedEdge] with Serializable {

  override def compare(o1: ValuedEdge, o2: ValuedEdge): Int = {

    if (o1.u < o2.u) {
      return -1
    } else if (o1.u > o2.u) {
      return 1
    } else {
      if (o1.value < o2.value) {
        return -1
      } else if (o1.value > o2.value) {
        return 1
      } else return 0
    }
  }
}
