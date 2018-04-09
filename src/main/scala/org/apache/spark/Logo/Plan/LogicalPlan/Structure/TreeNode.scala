package org.apache.spark.Logo.Plan.LogicalPlan.Structure

import org.apache.spark.Logo.Plan.LogicalPlan.AttributesMap

abstract class TreeNode(){
  def children():Seq[TreeNode]
  def treeString() = {
    val stringBuilder = new StringBuilder
    _treeString(stringBuilder,0)
    stringBuilder.toString()
  }
  def _treeString(stringBuilder: StringBuilder, level:Int):Unit = {

    stringBuilder.append("\n")
    for (i <-  0 until level) {
      stringBuilder.append("-")
    }
    stringBuilder.append(s"${toString}")
    for (node <- children()){
      stringBuilder.append(node._treeString(stringBuilder,level+1))
    }
  }
}

abstract class LogicalTreeNode extends TreeNode{
  def name:String
  def cost():Double

}

abstract class UnaryNode(val child:TreeNode) extends LogicalTreeNode{
  override def children(): Seq[TreeNode] = Seq(child)
}

abstract class LeafNode extends LogicalTreeNode{
  override def children(): Seq[TreeNode] = Nil
}

abstract class BinaryNode(val lChild:TreeNode, val rChild:TreeNode) extends LogicalTreeNode{
  override def children(): Seq[TreeNode] = Seq(lChild,rChild)
}
