package org.apache.spark.Logo.Plan.LogicalPlan.Structure

import org.apache.spark.Logo.UnderLying.utlis.ImmutableGraph

import scala.collection.mutable.ArrayBuffer

class GHDTree(val id:Int, var nodes:Map[Int,GHDNode], var graph:ImmutableGraph, val relationIDs:ArrayBuffer[Int], val attributeIDs:ArrayBuffer[Int]){


  //trim the redundant GHDNodes
  def trimTree(): Unit ={
    val ghdNode = nodes.values.toSeq
    val redundantNodes = ghdNode.flatMap{f =>
      ghdNode.map{g =>
        g.id == f.id match {
          case true => (g,f,false)
          case false => {
            g.contains(f) && f.contains(g) match {
              case true =>  {
                g.id < f.id match {
                  case true => (g,f,g.contains(f))
                  case false => (g,f,false)
                }
              }
              case false => (g,f,g.contains(f))
            }
          }
        }
      }.filter(_._3)}

    val redundantNodeSets = redundantNodes.map(_._2).toSet

//    println(nodes)
//    println(redundantNodeSets)

    //trim nodes
    nodes = nodes.filter(p => !redundantNodeSets.contains(p._2))
//    println(nodes)

    //trim graphs
    redundantNodes.foreach{case(l,r,_) => graph = graph.mergeNode(l.id,r.id)}
  }

  //optimize node of GHD to contain all the edges of attribute induced
  def attributeInducedVersion():GHDTree = {

    nodes.values.foreach(_.toCompleteAttributeNode())
    this
  }

  def fhw() = {
    val fractionalCovers = nodes.values.map(_.estimatedAGMCardinality()._2)
    (fractionalCovers.max, fractionalCovers.sum)
  }

  def isValid():Boolean = {
    def isAttributesInducedGraphConnected(attributeID:Int):Boolean = {
      val relatedNodesID = nodes.filter(f => f._2.attributeIDs.contains(attributeID)).map(_._1).toSeq
      graph.nodeInducedSubgraph(relatedNodesID).isConnected()
    }

    def containsCycle():Boolean = {
      graph.containsCycle()
    }

//    println(attributes.forall(p => isAttributesInducedGraphConnected(p)))
//    println(!containsCycle())
    attributeIDs.forall(p => isAttributesInducedGraphConnected(p)) && !containsCycle()
  }


  def addNode(node:GHDNode):Seq[GHDTree] = {

    nodes.isEmpty match {
      case true => {
        Seq(GHDTree(this,node,Seq()))
      }

      case false => {
        val nodeSet = nodes.values
        val possibleEdges = nodeSet.map(f => (node.id,f.id, !f.intersect(node)._1.isEmpty)).filter(_._3).map(f => (f._1,f._2)).toSeq

        var allCombinations = possibleEdges.combinations(1)
        for(i <- 2 to possibleEdges.size){
          allCombinations = allCombinations ++ possibleEdges.combinations(i)
        }

        allCombinations.map(f => GHDTree(this,node,f)).toSeq
      }
    }

  }

  override def toString: String = {
    if (nodes.isEmpty){
      s"${this.getClass.getSimpleName} id:${id}"
    } else{
      s"${this.getClass.getSimpleName} id:${id} nodes:${nodes.map(_.toString()).reduce(_ + _)} graph:${graph}"
    }
  }

}

object GHDTree{
  private var treeCount = 0
  def apply(oldTree:GHDTree, newNode:GHDNode, newEdges:Seq[(Int,Int)]) = {
    treeCount += 1
    val curID = treeCount - 1
    val newNodes = oldTree.nodes + ((newNode.id, newNode))
    val newGraph = oldTree.graph.addEdges(newEdges)
    val newRelations = (oldTree.relationIDs ++ newNode.relationIDs).distinct
    val newAttributes = (oldTree.attributeIDs ++ newNode.attributeIDs).distinct

    new GHDTree(curID, newNodes, newGraph, newRelations, newAttributes)
  }

  def apply():GHDTree = {
    treeCount += 1
    val curID = treeCount - 1
    new GHDTree(curID, Map(), ImmutableGraph(), ArrayBuffer(), ArrayBuffer())
  }
}
