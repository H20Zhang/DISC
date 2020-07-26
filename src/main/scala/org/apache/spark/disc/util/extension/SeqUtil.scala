package org.apache.spark.disc.util.extension

object SeqUtil extends Serializable {

  //value class combined with implicit as a method
  // to avoid object creation and inline the function call
  implicit class SubsetGen[A](val seq: Seq[A]) extends AnyVal {
    def subset[A]() = {
      val size = seq.size
      Range(1, size + 1).flatMap { subsetSize =>
        seq.combinations(subsetSize)
      }
    }
  }

  def subset[A](seq: Seq[A]): Seq[Seq[A]] = {
    val size = seq.size
    Range(1, size + 1).flatMap { subsetSize =>
      seq.combinations(subsetSize)
    }
  }
}
