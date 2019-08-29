package org.apache.spark.adj.utils

object SeqHelper extends Serializable {


  def subset[A](seq:Seq[A]):Seq[Seq[A]] = {
    val size = seq.size
    Range(1,size+1).flatMap{
      subsetSize => seq.combinations(subsetSize)
    }
  }
}
