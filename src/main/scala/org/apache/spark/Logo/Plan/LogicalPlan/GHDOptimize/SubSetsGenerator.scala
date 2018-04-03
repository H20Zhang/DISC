package org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize

import org.apache.spark.Logo.Plan.LogicalPlan.Structure.RelationSchema
import org.apache.spark.Logo.UnderLying.utlis.PointToNumConverter

import scala.collection.mutable.ArrayBuffer

class SubSetsGenerator(relation:ArrayBuffer[Int]) {





  def enumerateSet():ArrayBuffer[ArrayBuffer[ArrayBuffer[Int]]] = {
    val res = _enumerateSet(ArrayBuffer[ArrayBuffer[Int]](),ArrayBuffer[Int](),relation)
    res
  }


  //using ordering to eliminate redundant subsets
  def _arrayOrder(lhs:ArrayBuffer[Int], rhs:ArrayBuffer[Int]):Boolean = {

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
  def _subsetOrder(lhs: ArrayBuffer[Int], rhs:ArrayBuffer[Int]):Boolean = {
    if (lhs.size < rhs.size){
      return true
    } else if(rhs.size < lhs.size) {
      return false
    } else {
      return _arrayOrder(lhs,rhs)
    }

  }



  //test if the newly added GHD node will form a cycle with the previous added nodes.
  def _isTree(prevNodes:ArrayBuffer[ArrayBuffer[Int]], curNode:ArrayBuffer[Int]):Boolean = {
    val connectedNodes = ArrayBuffer[ArrayBuffer[Int]]()

    for (s <- prevNodes){
      if (_isConnected(s,curNode)){
        connectedNodes += s
      }
    }

    connectedNodes.combinations(2).foreach{
      p =>
        val lhs = p(0)
        val rhs = p(1)
        if (_isConnected(lhs,rhs)){
          return true
        }

    }
    false
  }


  //TODO:need to implement connected component algorithm and isTree algorihtm in graph


  // test if two nodes in the GHD are connected using the definition of the GHD (Two nodes will have an edge, if they share common attributes)
  def _isConnected(lhs:ArrayBuffer[Int], rhs:ArrayBuffer[Int]):Boolean = {

    val relationSchema = RelationSchema.getRelationSchema()
    val lRelations = lhs.map(relationSchema.getRelation)
    val rRelations = rhs.map(relationSchema.getRelation)
    val lAttributes = lRelations.flatMap(_.attributes).distinct
    val rAttributes = rRelations.flatMap(_.attributes).distinct

    ! lAttributes.intersect(rAttributes).isEmpty
  }

  // test if the relations in a single GHD is connected
  def _isDisconnected(node:ArrayBuffer[Int]):Boolean = {
    val relationSchema = RelationSchema.getRelationSchema()
    val relations = node.map(relationSchema.getRelation)

    relations.foreach{
      f =>

        val filteredRelations = relations.filter(_ != f)

        if (filteredRelations.size != 0){
          val res = filteredRelations.forall{
            t =>
              f.attributes.intersect(t.attributes).isEmpty
          }

          if (res){
            return true
          }
        }

    }

    false
  }

  def _enumerateSet(prevNodes:ArrayBuffer[ArrayBuffer[Int]],prevSelected:ArrayBuffer[Int], relation:ArrayBuffer[Int]):ArrayBuffer[ArrayBuffer[ArrayBuffer[Int]]] = {

    val res = ArrayBuffer[ArrayBuffer[ArrayBuffer[Int]]]()


    if (relation.size > 1){
      for (i <- 1 to relation.size){
        val subarrays = selectIelementes(i,relation).filter(p => _subsetOrder(prevSelected,p))

        for (j <- subarrays){


          //test if curNode will form cycle with previous nodes
          if (!_isDisconnected(j) && !_isTree(prevNodes,j)){
            val remainRelations = relation.diff(j)
            val subsets = _enumerateSet(prevNodes :+ j,j, remainRelations)

            if (subsets.size != 0){
              for (k <- subsets){
                res += j +: k
              }
            } else if (j.size == relation.size){
              res += ArrayBuffer(j)
            }
          }

        }
      }
      return res

    } else if (relation.size == 1) {
      if (_subsetOrder(prevSelected,relation)) {
        val temp = ArrayBuffer[ArrayBuffer[Int]](relation)
        res += temp
      }


      return res
    } else {
      res
    }

  }

  def selectIelementes(k:Int, relations:ArrayBuffer[Int]) ={
    relations.combinations(k)
  }

}
