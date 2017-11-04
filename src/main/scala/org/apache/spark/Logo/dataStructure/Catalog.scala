package org.apache.spark.Logo.dataStructure

import org.apache.spark.rdd.RDD

import scala.collection.mutable

class Catalog {
  val rddMap:mutable.Map[String,RDD[LogoBlock[_]]] = mutable.Map()

  def putLogo(name:String , rdd:RDD[LogoBlock[_]]): Unit ={
    rddMap += ((name,rdd))
  }

  def getLogo(name:String) ={
    rddMap(name)
  }

  def removeLogo(name:String): Unit ={
    rddMap -= name
  }

}
