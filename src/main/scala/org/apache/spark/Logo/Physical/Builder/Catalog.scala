package org.apache.spark.Logo.Physical.Builder

import org.apache.spark.Logo.Physical.dataStructure.{LogoBlock, LogoSchema, RowLogoBlock}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag

class Catalog {
  val rddMap:mutable.Map[String,LogoRDDReference] = mutable.Map()

  def putLogo(name:String , rdd:LogoRDDReference): Unit ={
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

  def putLogo(name:String , rdd:LogoRDDReference): Unit ={
    _catalog.rddMap += ((name,rdd))
  }

  def getLogo(name:String) ={
    _catalog.rddMap(name)
  }

  def removeLogo(name:String): Unit ={
    _catalog.rddMap -= name
  }


}

class LogoRDDReference()
case class RowLogoRDDReference[A:ClassTag](logoRDD:RDD[RowLogoBlock[A]], schema: LogoSchema) extends LogoRDDReference



