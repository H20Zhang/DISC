package org.apache.spark.Logo.Physical.utlis



//select i-th element from seq, required by keys
object ListSelector {

  def selectElements[A](elems:Seq[A], keys:Seq[Int]):Seq[A] = {
    val keySet = keys.toSet
    elems.zipWithIndex.filter(p => keySet.contains(p._2)).map(_._1)
  }

  def notSelectElements[A](elems:Seq[A], keys:Seq[Int]):Seq[A] = {
    val keySet = keys.toSet
    elems.zipWithIndex.filter(p => !keySet.contains(p._2)).map(_._1)
  }
}
