package org.apache.spark.Logo.Plan

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class QTree(root:QTreeNode) {
  def printTree(): Unit ={
    val stringBuilder = new StringBuilder
    root.printTree(stringBuilder,0)
    println(stringBuilder)
  }

  def treeString:String = {
    val stringBuilder = new StringBuilder
    root.printTree(stringBuilder,0)
    stringBuilder.toString()
  }

  def leftDeepTraverse(f:QTreeNode => Unit): Unit ={
    root.leftDeepTraverse(f)
  }

  def costString(patternSizeMap:Map[String,Int], lazyMap:Map[String,Boolean]) = {
    root.costComputationString(patternSizeMap,lazyMap)
  }

}


class QTreeNode(_name:String, _attributes:Seq[Int], val lNode:QTreeNode, val rNode:QTreeNode){

  def printTree(stringBuilder: StringBuilder, level:Int):Unit = {

    stringBuilder.append("\n")
    for (i <-  0 until level) {
      stringBuilder.append(" ")
    }
    stringBuilder.append(s"name:$name,attr:${AttributesMap.translateBack(_attributes)}")
    if (lNode != null){
      lNode.printTree(stringBuilder,level +1)
    }

    if (rNode != null){
      rNode.printTree(stringBuilder,level +1)
    }
  }

  def attr:Seq[String] = {
    val res = AttributesMap.translateBack(_attributes)
    if (res == null){
      mutable.Seq[String]()
    } else{
      res
    }

  }

  def name:String = {
    _name
  }

  def leftDeepTraverse(f:QTreeNode => Unit): Unit ={
    f(this)
    if (lNode != null){
      lNode.leftDeepTraverse(f)
    }

    if (rNode != null){
      rNode.leftDeepTraverse(f)
    }
  }


  lazy val variblesIntersection = {
    if (lNode != null && rNode != null){
      AttributesMap.translate(lNode.attr.intersect(rNode.attr))
    } else{
      Seq[Int]()
    }
  }

  lazy val variblesUnion =
  {
    if (lNode != null && rNode != null){
      AttributesMap.translate(lNode.attr.union(rNode.attr).distinct)
    } else if (lNode != null){
      AttributesMap.translate(lNode.attr)
    } else if (rNode != null){
      AttributesMap.translate(rNode.attr)
    } else{
      Seq[Int]()
    }
  }

  lazy val lUniqueVaribles = AttributesMap.translate(lNode.attr).diff(variblesIntersection)
  lazy val rUniqueVaribles = AttributesMap.translate(rNode.attr).diff(variblesIntersection)

  def multipleVaribles(varibles:Seq[Int]) = {
    val stringBuilder1 = new StringBuilder()
    varibles.foldLeft(stringBuilder1)((builder,s) => builder.append(s"*x($s)"))
    stringBuilder1.toString()
    if (stringBuilder1.size == 0){
      ""
    } else{
      stringBuilder1.deleteCharAt(0).toString()
    }
  }

  def localJoinCost(costMap:Map[String,Int]) = {
    costMap(name)
  }

  def eagerComputationCostString(patternSizeMap:Map[String,Int], lazyMap:Map[String,Boolean]):String = {
    val stringBuilder = new mutable.StringBuilder()

    stringBuilder.append(s"(${patternSizeMap("Computation")}*${localJoinCost(patternSizeMap)}")

    if (lNode != null && !lazyMap(lNode.name)){

      if (multipleVaribles(rUniqueVaribles) != ""){
        stringBuilder.append(s" + ${lNode.eagerComputationCostString(patternSizeMap,lazyMap)}*${multipleVaribles(rUniqueVaribles)}")
      } else{
        stringBuilder.append(s" + ${lNode.eagerComputationCostString(patternSizeMap,lazyMap)}")
      }

    }

    if (rNode != null && !lazyMap(rNode.name)){
      if (multipleVaribles(lUniqueVaribles) != ""){
        stringBuilder.append(s" + ${rNode.eagerComputationCostString(patternSizeMap,lazyMap)}*${multipleVaribles(lUniqueVaribles)}")
      } else{
        stringBuilder.append(s" + ${rNode.eagerComputationCostString(patternSizeMap,lazyMap)}")
      }
    }
    stringBuilder.append(")")

    stringBuilder.toString()
  }

  def costComputationString(patternSizeMap:Map[String,Int], lazyMap:Map[String,Boolean]):String = {
    val stringBuilder = new mutable.StringBuilder()

    if (lazyMap(name)){

        stringBuilder.append("("+eagerComputationCostString(patternSizeMap,lazyMap) + s")/(${multipleVaribles(variblesUnion)})*ceil(${multipleVaribles(variblesUnion)}/${patternSizeMap("Machine")})")

      if (lNode != null && lazyMap(lNode.name)){
        if (lNode.name != "Edge") {
          stringBuilder.append("+" + lNode.costComputationString(patternSizeMap, lazyMap))
        }
      }
      if (rNode != null && lazyMap(rNode.name)){
        if (rNode.name != "Edge") {
          stringBuilder.append("+" + rNode.costComputationString(patternSizeMap, lazyMap))
        }
      }
    }
    stringBuilder.toString()
  }

}



object AttributesMap{
  private val theMap:mutable.Map[String,Int] = new mutable.HashMap[String,Int]()
  private val reverseMap:mutable.Map[Int,String] = new mutable.HashMap[Int,String]()
  private var curEle:Int = 0

  def getMap():mutable.Map[String,Int] = {
    theMap
  }

  def translate(attributes:Seq[String]):Seq[Int] = {

    val arrayBuffer = new ArrayBuffer[Int]()

    attributes.foreach(f =>
      theMap.get(f) match {
        case Some(v) => arrayBuffer += v
        case _ => curEle += 1; theMap.put(f,curEle); reverseMap.put(curEle,f); arrayBuffer += curEle
      }
    )

    arrayBuffer
  }

  def translateBack(attributes:Seq[Int]):Seq[String] = {
    val arrayBuffer = new ArrayBuffer[String]()

    attributes.foreach(f =>
      reverseMap.get(f) match {
        case Some(v) => arrayBuffer += v
        case _ =>
      }
    )

    arrayBuffer
  }
}



object QTreeNode{
  def apply(name: String,lNode: QTreeNode,rNode: QTreeNode): QTreeNode = new QTreeNode(
  name,AttributesMap.translate(lNode.attr.union(rNode.attr).distinct),lNode,rNode)

  def apply(name: String,attributes: Seq[String]): QTreeNode = new QTreeNode(
    name,AttributesMap.translate(attributes),null,null
  )

  def EdgeNode(attribute:(String,String)) = apply("Edge",Seq(attribute._1,attribute._2))
}





