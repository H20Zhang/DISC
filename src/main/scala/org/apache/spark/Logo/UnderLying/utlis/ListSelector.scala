package org.apache.spark.Logo.UnderLying.utlis

import scala.collection.mutable.ArrayBuffer


//select i-th element from seq, required by keys
object ListSelector {

  def selectElements[A](elems: Seq[A], keys: Set[Int]): Seq[A] = {
    val keySet = keys

    val arraybuffer = new ArrayBuffer[A]()
    for (i <- 0 until elems.size) {
      if (keySet.contains(i))
        arraybuffer.append(elems(i))
    }

    arraybuffer
    //    elems.zipWithIndex.filter(p => keySet.contains(p._2)).map(_._1)
  }

  def notSelectElements[A](elems: Seq[A], keys: Set[Int]): Seq[A] = {

    val keySet = keys

    val arraybuffer = new ArrayBuffer[A]()
    for (i <- 0 until elems.size) {
      if (!keySet.contains(i))
        arraybuffer.append(elems(i))
    }

    arraybuffer
  }
}
