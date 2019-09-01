package org.apache.spark.adj.utils.extension

import scala.collection.mutable.ArrayBuffer

object ArrayUtil {

//  find all enumeration order of the element of the list
  def enumerations[A](elements: Seq[A]): Seq[Seq[A]] = {

    if (elements.isEmpty) {
      ArrayBuffer[ArrayBuffer[A]]()
    } else {
      val enumArrays = elements.flatMap { element =>
        val remainElements = elements.filter(p => p != element)

        if (remainElements.isEmpty) {
          ArrayBuffer(ArrayBuffer(element))
        } else {
          val remainEnumerations = enumerations(remainElements)
          remainEnumerations.map(f => element +: f)
        }
      }

      enumArrays
    }
  }

//  find powerset(all subsets) of a list
  def powerset[A](elements: Seq[A]): Seq[Seq[A]] = {
    val n = elements.size
    Range(0, n).flatMap { size_ =>
      val size = size_ + 1; elements.combinations(size)
    }
  }

  //    cartesian produce over two list
  def catersianProduct[A](l: Seq[Seq[A]], r: Seq[Seq[A]]): Seq[Seq[A]] = {
    l.flatMap { ele1 =>
      r.map { ele2 =>
        ele1 ++ ele2
      }
    }
  }

}
