package org.apache.spark.Logo.Plan.LogicalPlan.Structure

import scala.collection.mutable.ArrayBuffer

class GHDNode(val id:Int, val relations:ArrayBuffer[Int], val attributes:ArrayBuffer[Int], var nexts:Seq[TreeNode]) extends TreeNode{
  override def children(): Seq[TreeNode] = nexts
}

object GHDNode{
  private var nodeCount = 0
  def apply(tree:GHDTree, relations:ArrayBuffer[Int], attributes:ArrayBuffer[Int]):GHDNode = {
    nodeCount += 1
    new GHDNode(nodeCount-1,relations, attributes, null)
  }
}


class GHDTree(id:Int, root:GHDNode, nodes:Map[Int,GHDNode], links:Map[Int,Seq[Int]], relations:ArrayBuffer[Int], attributes:ArrayBuffer[Int]){

  def isConnected(lID:Int, rID:Int):Boolean = ???
  def isConnected(nodeIDs:Seq[Int]):Boolean = ???
  def canAddNode(node:GHDNode):Boolean = ???
  def addNode(node:GHDNode):Boolean = ???

}

object GHDTree{
  private var treeCount = 0
  def apply(root:GHDNode) = {
    treeCount += 1
    val curID = treeCount - 1
    new GHDTree(curID, root, Map((root.id,root)), Map(), root.relations, root.attributes)
  }
}