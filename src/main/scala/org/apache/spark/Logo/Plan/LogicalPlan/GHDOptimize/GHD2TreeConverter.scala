package org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize


import org.apache.spark.Logo.Plan.LogicalPlan.Structure._

import scala.collection.mutable.ArrayBuffer

class GHD2TreeConverter() {

  val subSetsGenerator = new SubSetsGenerator(ArrayBuffer(1))

  //generate a leftDeep Tree from GHD tree
  def GHD2LeftDeepTree(GHD:ArrayBuffer[ArrayBuffer[Int]]):Seq[LogicalTreeNode] = {

    val logicalTreeNodes = new ArrayBuffer[LogicalTreeNode]()
    for (g <- GHD.permutations){
      val length = g.length
      var root:LogicalTreeNode = null

      for(i <- 0 until length){

        if (i == 0){
          val tempNode = new GJJoinNode(g(i))
          root = tempNode
        }  else {
          root match {
            case r:GJJoinNode => {
              if (subSetsGenerator.hasEdgeBetweenGHDNode(r.relations,g(i))){
                val tempNode = new GJJoinNode(g(i))
                val tempNode1 = new HashJoinNode(r,tempNode)
                root = tempNode1
              } else {
                root = null
              }
            }
            case r:HashJoinNode => {
              if ((0 until i).exists(p => subSetsGenerator.hasEdgeBetweenGHDNode(g(p),g(i)))){
                val tempNode = new GJJoinNode(g(i))
                val tempNode1 = new HashJoinNode(r,tempNode)
                root = tempNode1
              } else {
                root = null
              }
            }
            case _ =>
          }
        }
      }

      if (root != null){
        logicalTreeNodes += root
      }

    }

    logicalTreeNodes
  }
}

class GJJoinNode(val relations:ArrayBuffer[Int]) extends LeafNode{

  lazy val schema = RelationSchema.getRelationSchema()
  override def cost(): Double = ???
  override def name: String = "GenericJoin"

  override def toString: String = s"$name ${relations.map(schema.getRelation).map(_.name)}"

}

class HashJoinNode(lNode:LogicalTreeNode, rNode:LogicalTreeNode) extends BinaryNode(lNode,rNode){
  override def cost(): Double = ???

  override def name: String = "HashJoin"

  override def toString: String = s"$name L:${lNode.name} R:${rNode.name}"
}