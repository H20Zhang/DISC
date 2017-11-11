package org.apache.spark.Logo.Physical.utlis

import scala.reflect.ClassTag

object TestUtil {
  def listEqual[A:ClassTag](lList:Seq[A], rList:Seq[A]):Boolean = lList.zip(rList).forall(f => f._1 == f._2)
  def objectEqual[A:ClassTag, B:ClassTag](l:A,r: B) = l == r


}
