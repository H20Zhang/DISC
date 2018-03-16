package org.apache.spark.Logo.Plan.LogicalPlan

import scala.collection.mutable.ArrayBuffer

class GHDDecomposer(edges:Seq[(Int,Int)]) {


  var GHDTrees:ArrayBuffer[GHDTreeNode] = new ArrayBuffer[GHDTreeNode]()

  def generateKWidthGHDNodes(remaindEdges:Seq[(Int,Int)], k:Int):Seq[(GHDTreeNode,Seq[(Int,Int)])] = ???
  def generateLessThanKWitdthGHD(k:Int, remainEdges:Seq[(Int,Int)], prevTree:GHDTreeNode):Unit = {

    if (remainEdges.size > 0){
      for (i <- 1 to k){
        val GHDNodes = generateKWidthGHDNodes(remainEdges,i)
        for (node <- GHDNodes){
          if (connectNode(prevTree,node._1)){
            generateLessThanKWitdthGHD(k,node._2,node._1)
          }
        }
      }
    } else{
     GHDTrees += prevTree
    }
  }

  def connectNode(preTree:GHDTreeNode, newNode:GHDTreeNode):Boolean = ???

}

case class GHDTreeNode(child:Seq[GHDTreeNode], variables:Seq[Int], Relations:Seq[(Int,Int)]);

class GHDTreeRoot(){

}

class GHDTreeOptimizer(GHDTrees:ArrayBuffer[GHDTreeNode],edges:Seq[(Int,Int)]){

}
