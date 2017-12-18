package org.apache.spark.Logo.Physical.Builder

import org.apache.spark.Logo.Physical.dataStructure._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * catlog used to associate a name with a RDD[LogoRDDReference],
  * LogoRDDReference records the RDD[LogoBlock] and its according schema
  */
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

  def getLogo[A](name:String) ={
    _catalog.getLogo(name).logoRDD.asInstanceOf[A]
  }

  def removeLogo(name:String): Unit ={
    _catalog.rddMap -= name
  }

}



//TODO actually logoRDD field should be removed it is generated through generate, before generate the logoRDD doesn't exist, LogoRDDReference is a placeholder.
/**
  * a reference to the logoRDD containing its actual rdd and its schema
  */
case class LogoRDDReference(logoRDD:RDD[LogoBlockRef], schema: LogoSchema, buildScriptStep: LogoBuildScriptStep){
  def generate() = ???
}


//TODO finish this, the same case as LogoRDDReference
case class PatternLogoRDDReference(patternRDD:RDD[LogoBlockRef], patternSchema: LogoSchema, buildScript:LogoPatternBuildScriptOneStep) extends LogoRDDReference(patternRDD,patternSchema,buildScript){

  //prepare the Pattern Logo RDD for build operation.
  def toSubPattern(keyMapping: KeyMapping):SubPatternLogoRDDReference = ???

  //actually generate the patternRDD
  def generate() = ???

}


case class SubPatternLogoRDDReference(patternLogoRDDReference:PatternLogoRDDReference, keyMapping:KeyMapping){

}

