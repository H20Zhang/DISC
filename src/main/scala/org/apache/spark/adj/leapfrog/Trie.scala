package org.apache.spark.adj.leapfrog

import java.util.Comparator

import org.apache.spark.adj.database.Database.DataType

import scala.collection.mutable.ArrayBuffer

trait Trie {
  def nextLevel(binding:ArraySegment):ArraySegment
  def toRelation():Array[Array[DataType]]
}

// edge:Array[(ID, ID, Value)], node:Array[(Start, End)]
class ArrayTrie(neighbors:Array[Int], values:Array[Int], neighborBegins:Array[Int], neighborEnds:Array[Int], level:Int) extends Trie {

  def nextLevel(binding: ArraySegment):ArraySegment = {

    var id = 0
    var start = neighborBegins(id)
    var end = neighborEnds(id)

//    println(s"binding:${binding}")
//    println(s"value:${values.toSeq}")
//    println(s"start:${start}, end:${end}")

    val level = binding.size
    var i = 0

    while(i < level){

//      println(s"${i}-th binding:${binding(i)}")
//      println(s"start:${start}, end:${end}")
      val pos = Alg.binarySearch(values, binding(i), start, end)

      if (pos == -1){
        return ArraySegment.emptyArray()
      }

      id = neighbors(pos)

//      println(s"level:${i}")
//      println(s"start:${start}, end:${end}")
//      println(s"pos:${pos}")
//      println(s"id:${id}")

      start = neighborBegins(id)
      end = neighborEnds(id)
      i += 1
    }


    ArraySegment(values, start, end, end-start)
  }

  //just for verify the correctness of the trie implementation
  def toRelation():Array[Array[DataType]] = {
    var tables = nextLevel(ArraySegment.emptyArray()).toArray().map(f => Array(f))

    var i = 1
    while(i < level){
      tables = tables.flatMap{
        f =>
          val nextLevelValues = nextLevel(ArraySegment(f))
          nextLevelValues.toArray().map(value => f:+value)
      }

//      println(s"table of ${i}-th level is: ${tables.toSeq.map(_.toSeq)}")
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

class HashMapTrie extends Trie {
  override def nextLevel(binding: ArraySegment): ArraySegment = ???

  override def toRelation(): Array[Array[DataType]] = ???
}

object ArrayTrie {
  def apply(table:Array[Array[DataType]], arity:Int): ArrayTrie = {

    //sort the relation in lexical order
    val comparator = new LexicalOrderComparator(arity)
    java.util.Arrays.sort(table, comparator)

    //init
    var idCounter = 0
    var leafIDCounter = -1
    val prevIDs = new Array[Int](arity)
    var prevValue = new Array[Int](arity)
    val edgeBuffer = ArrayBuffer[(Int, Int, DataType)]()
    val nodeBuffer = ArrayBuffer[(Int, Int)]()
    val tableSize = table.size

    var i = 0
    while(i < arity){
      prevIDs(i) = 0
      prevValue(i) = Int.MaxValue
      i += 1
    }


    //construct edge for ArrayTrie
    val rootID = idCounter
    idCounter += 1

    i = 0
    while(i < tableSize){
      val curTuple = table(i)

      //find the j-th position where value of curTuple diverage from prevValue
      var j = -1
      var isJContinue = true
      while(isJContinue){
        j += 1
        if (j >= arity){
          isJContinue = false
        } else {
          if (curTuple(j) != prevValue(j)){
            isJContinue = false
          }
        }
      }

      j = j-1

      while (j < arity-1){
        var prevID = 0

        if (j == -1){
          prevID = rootID
        } else {
          prevID = prevIDs(j)
        }

        var newID = 0

        if(j < arity - 2){
          newID = idCounter
          idCounter += 1
        } else {
          newID = leafIDCounter
          leafIDCounter -= 1
        }


        edgeBuffer += ((prevID, newID, curTuple(j+1)))
        prevIDs(j+1) = newID

        j += 1
      }

      prevValue = curTuple
      i += 1
    }

    //add a tuple to mark the end of the edge
    edgeBuffer += ((Int.MaxValue, Int.MaxValue, Int.MaxValue))
    //sort edge first by "id" then "value"
    val edge = edgeBuffer.toArray
    val edgeComparator = new EdgeComparator
    java.util.Arrays.sort(edge, edgeComparator)


//    println(s"sorted edges:${edge.toSeq}, size:${edge.size}")
    //construct node for ArrayTrie
    i = 0
    var start = 0
    var end = 0
    var currentValue = 0

    while (i < edge.size){
      if (edge(i)._1 == currentValue){
        end += 1
      } else {
        nodeBuffer += ((start,end))

        currentValue = edge(i)._1
        start = i
        end = i+1
      }
      i += 1
    }

    new ArrayTrie(edge.dropRight(1).map(_._2), edge.dropRight(1).map(_._3), nodeBuffer.map(_._1).toArray, nodeBuffer.map(_._2).toArray, arity)
  }
}

class LexicalOrderComparator(attrNum:Int) extends Comparator[Array[DataType]] with Serializable {

  override def compare(o1: Array[DataType], o2: Array[DataType]): Int = {
    var i = 0
    while(i < attrNum){
      if (o1(i) < o2(i)){
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

class EdgeComparator extends Comparator[(Int,Int,DataType)] with Serializable {

  override def compare(o1: (Int,Int,DataType), o2: (Int,Int,DataType)): Int = {

    if (o1._1 < o2._1){
      return -1
    } else if (o1._1 > o2._1){
      return 1
    } else {
      if (o1._3 < o2._3){
        return -1
      } else if (o1._3 > o2._3){
        return 1
      } else return 0
    }
  }
}