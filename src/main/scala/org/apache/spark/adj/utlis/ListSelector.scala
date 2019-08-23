package org.apache.spark.adj.utlis

import org.apache.spark.adj.deprecated.execution.rdd.PatternInstance

import scala.collection.mutable.ArrayBuffer


//select i-th element from seq, required by keys
object ListSelector {

  def selectElements[A](elems: Seq[A], keys: Set[Int]): Seq[A] = {
    val keySet = keys

    val arraybuffer = new ArrayBuffer[A]()

    var i = 0
    var size = elems.length
    while (i < size){
      if (keySet.contains(i))
        arraybuffer.append(elems(i))
      i += 1
    }

    arraybuffer
    //    elems.zipWithIndex.filter(p => keySet.contains(p._2)).map(_._1)
  }

  def notSelectElements[A](elems: Seq[A], keys: Set[Int]): Seq[A] = {

    val keySet = keys

    val arraybuffer = new ArrayBuffer[A]()
    var i = 0
    var size = elems.length
    while (i < size){
      if (!keySet.contains(i))
        arraybuffer.append(elems(i))
      i += 1
    }

    arraybuffer
  }


  def notSelectElementsIntPattern(elems: PatternInstance, keys: Set[Int]): Seq[Int] = {

    val keySet = keys

    val arraybuffer = new ArrayBuffer[Int]()

    var i = 0
    var size = elems.size()
    while (i < size){
      if (!keySet.contains(i))
        arraybuffer.append(elems.getValue(i))
      i += 1
    }

    arraybuffer
  }
}
