package org.apache.spark.Logo.Plan.LogicalPlan.Structure

import org.apache.spark.Logo.Plan.LogicalPlan.AttributesMap

import scala.collection.mutable.ArrayBuffer

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
  def setP(AllP:Seq[Int]):Unit
  def getP():Seq[(Int,Int)]
  def attributes:Seq[Int]
  def relations:Seq[Int]

  def cost(costFunc:(this.type) => Double):Double = {
    costFunc(this)
  }
  def size(sizeFunc:(this.type) => Double):Double = {
    sizeFunc(this)
  }

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


class GJJoinNode(_relations:ArrayBuffer[Int]) extends LeafNode{

  lazy val schema = RelationSchema.getRelationSchema()
  lazy val _attributes = attributes
  var _P:Seq[(Int,Int)] = null

  override def name: String = "GenericJoin"

  override def toString: String = s"$name ${relations.map(schema.getRelation).map(_.name)}"

  override def setP(allP:Seq[Int]) {
    _P = _attributes.map(f => (f,allP(f)))
  }

  override def getP(): Seq[(Int, Int)] = _P match {
    case r:Seq[(Int,Int)] => r
    case _ => assert(true, "P must be set before calling getP"); null
  }

  override def attributes: Seq[Int] = _relations.map(schema.getRelation).flatMap(_.attributes.map(schema.getAttribute)).distinct

  override def relations: Seq[Int] = _relations

}

class HashJoinNode(lNode:LogicalTreeNode, rNode:LogicalTreeNode) extends BinaryNode(lNode,rNode){


  lazy val _attributes = attributes
  var _P:Seq[(Int,Int)] = null

  override def name: String = "HashJoin"

  override def toString: String = s"$name L:${lNode.name} R:${rNode.name}"

  override def setP(allP:Seq[Int]): Unit ={
    _P = _attributes.map(f => (f,allP(f)))
    children().foreach(f => {
      if (f.isInstanceOf[LogicalTreeNode]){
        f.asInstanceOf[LogicalTreeNode].setP(allP)
      }
    })
  }

  override def getP(): Seq[(Int, Int)] = _P match {
    case r:Seq[(Int,Int)] => r
    case _ => assert(true, "P must be set before calling getP"); null
  }

  override def attributes: Seq[Int] = lNode.attributes ++ rNode.attributes distinct

  override def relations: Seq[Int] = lNode.relations ++ rNode.relations distinct
}