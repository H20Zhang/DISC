package org.apache.spark.Logo.UnderLying.Catalog

import org.apache.spark.Logo.UnderLying.dataStructure._

import scala.collection.mutable

/**
  * catlog used to associate a name with a RDD[LogoRDDReference],
  * LogoRDDReference records the RDD[LogoBlock] and its according schema
  */
class Catalog {
  val rddMap:mutable.Map[String,LogoRDD] = mutable.Map()

  def putLogo(name:String , rdd:LogoRDD): Unit ={
    rddMap += ((name,rdd))
  }

  def getLogo(name:String) ={
    rddMap(name)
  }

  def removeLogo(name:String): Unit ={
    rddMap -= name
  }

}

object Catalog {
  lazy val _catalog = new Catalog

  def putLogo(name:String , rdd:LogoRDD): Unit ={
    _catalog.rddMap += ((name,rdd))
  }

  def getLogo[A](name:String) ={
    _catalog.getLogo(name).logoRDD.asInstanceOf[A]
  }

  def removeLogo(name:String): Unit ={
    _catalog.rddMap -= name
  }

}






