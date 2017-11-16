package org.apache.spark.Logo.Physical.utlis

import scala.reflect.ClassTag

object TestUtil {
  def listEqual[A:ClassTag](lList:Seq[A], listToVerify:Seq[A], msg:String = ""):Boolean = {

    val res = lList.zip(listToVerify).forall(f => f._1 == f._2)

    if (res == false){

      println(s"List String Equal Fail: ${msg}")
      listToVerify.foreach(println)
    }

    res
  }

  def listlistEqual[A:ClassTag](lList:Seq[Seq[A]], listToVerify:Seq[Seq[A]], msg:String = ""):Boolean = {
    lList.zip(listToVerify).forall(f => listEqual(f._1,f._2))
  }

  def objectEqual[A:ClassTag, B:ClassTag](l:A,r: B) = l == r

}
