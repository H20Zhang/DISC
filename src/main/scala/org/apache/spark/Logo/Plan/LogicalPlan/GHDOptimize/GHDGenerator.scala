package org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize

import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{GHDNode, GHDTree}

import scala.collection.mutable.ArrayBuffer


//generate fhw optimal GHD
class GHDGenerator(relations:ArrayBuffer[Int]) {
  //using ordering to eliminate redundant subsets
  private def _arrayOrder(lhs:ArrayBuffer[Int], rhs:ArrayBuffer[Int]):Boolean = {

    assert(lhs.size == rhs.size, "size of the two array must be the same")

    val k = lhs.size
    var i = 0

    while (i < k){
      if (lhs(i) < rhs(i)) {
        return true
      } else if (rhs(i) < lhs(i)){
        return  false
      }

      i += 1
    }

    return true

  }


  //using ordering to eliminate redundant subsets
  private def _subsetOrder(lhs: ArrayBuffer[Int], rhs:ArrayBuffer[Int]):Boolean = {

    if (lhs == null){
      return true
    } else if (rhs == null){
      return false
    } else if (lhs.size < rhs.size){
      return true
    } else if(rhs.size < lhs.size) {
      return false
    } else {
      return _arrayOrder(lhs,rhs)
    }

  }

  def fhwOptimalGHD() = {
    val gHDTrees = attributeInducedGHDSet()
    val optimalFHWTree = gHDTrees.minBy(_.fhw())
    optimalFHWTree.trimTree()
    (optimalFHWTree,optimalFHWTree.fhw())
  }

  def attributeInducedGHDSet():ArrayBuffer[GHDTree] = {
    GHDSet.map(_.attributeInducedVersion())
  }

  def GHDSet():ArrayBuffer[GHDTree] = {
    var res = ArrayBuffer[GHDTree]()
    _GHDSet(res, GHDTree(), GHDNode(), relations)
    res
  }

  private def _GHDSet(GHDTrees:ArrayBuffer[GHDTree],tree:GHDTree, prevSelected:GHDNode, relation:ArrayBuffer[Int]):Unit = {

    if (relation.size > 0){
      for (i <- 1 to relation.size){

//        println(s"current relation:${relation}")
        val subArrays = relation.combinations(i).filter(p => _subsetOrder(prevSelected.relationIDs, p)).toList

//        println(s"possible lists:${subArrays}")

        for (j <- subArrays){

//          println(j)
          val tempNode = GHDNode(j)
//          println(s"tempNode ${tempNode.relations}")
//          println(s"isConnected ${tempNode.isConnected()}")

          //test if curNode will form cycle with previous nodes
//          if (tempNode.isConnected()){
            val newGHDs = tree.addNode(tempNode)
            val remainRelations = relation.diff(j)

//            println(s"remainRelations:${remainRelations}")
//            println(s"currentTree: ${tree}")

            for (newTree <- newGHDs){
//              println(newTree)
//              println(newTree.isValid())
              if (newTree.isValid())
                _GHDSet(GHDTrees,newTree,tempNode, remainRelations)
            }
//          }
        }
      }
    }
    else {
      GHDTrees += tree
    }
  }
}
