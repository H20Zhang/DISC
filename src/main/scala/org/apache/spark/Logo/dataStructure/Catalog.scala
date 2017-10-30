package org.apache.spark.Logo.dataStructure

import org.apache.spark.rdd.RDD

import scala.collection.mutable

class Catalog {
  val rddMap:mutable.Map[String,RDD[ListLogoBlock[_]]] = mutable.Map()

  def addRDD(name:String , rdd:RDD[ListLogoBlock[_]]): Unit ={
    rddMap += ((name,rdd))
  }

  def deleteRDD(name:String): Unit ={
    rddMap -= name
  }

}
