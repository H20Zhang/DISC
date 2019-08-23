package org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.Structure

//class OrderedGHDPlans(val plans:Seq[GHDPlan], val nodes:Map[Int,GHDNode]) {
//
//  private val queryTimeMap:mutable.Map[(Int,Int),Double] = mutable.Map()
//  private val sizeTimeMap:mutable.Map[Int,(Double,Double)] = mutable.Map()
//
//  def updateQueryTime(key:(Int,Int), value:Double): Unit ={
//    queryTimeMap(key) = value
//  }
//
//  def retrieveQueryTime(key:(Int,Int)):Option[Double] = {
//    queryTimeMap.get(key)
//  }
//
//  def updateSizeTime(key:Int, value:(Double,Double)): Unit ={
//    sizeTimeMap(key) = value
//  }
//
//  def retrieveSizeTime(key:Int):Option[(Double,Double)] = {
//    sizeTimeMap.get(key)
//  }
//
//  def allSampleInformGenerate(k:Long) = {
//    plans.foreach(_.sampleInformationGenerate(k))
//  }
//
//  override def toString: String = {
//    val stringBuilder = new mutable.StringBuilder()
//    plans.foreach(f => stringBuilder.append(s"\n${f}"))
//    stringBuilder.toString()
//  }
//
//
//}
//
//class OrderedLaziedGHDPlans(val plans:Seq[OrderedLaziedGHDPlan], val nodes:Map[Int,GHDNode]) {
//  override def toString: String = {
//    val stringBuilder = new mutable.StringBuilder()
//    plans.foreach(f => stringBuilder.append(s"\n${f}"))
//    stringBuilder.toString()
//  }
//}