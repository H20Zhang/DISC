package org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize

import org.apache.spark.Logo.UnderLying.utlis.PointToNumConverter

import scala.collection.mutable.ArrayBuffer

class SubSetsGenerator(relation:ArrayBuffer[Int]) {





  def enumerateSet():ArrayBuffer[ArrayBuffer[ArrayBuffer[Int]]] = {
    val res = _enumerateSet(ArrayBuffer[Int](),relation)
    res
  }

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

  def _subsetOrder(lhs: ArrayBuffer[Int], rhs:ArrayBuffer[Int]):Boolean = {
    if (lhs.size < rhs.size){
      return true
    } else if(rhs.size < lhs.size) {
      return false
    } else {
      return _arrayOrder(lhs,rhs)
    }

  }

  def _enumerateSet(prevSelected:ArrayBuffer[Int], relation:ArrayBuffer[Int]):ArrayBuffer[ArrayBuffer[ArrayBuffer[Int]]] = {

    val res = ArrayBuffer[ArrayBuffer[ArrayBuffer[Int]]]()


    if (relation.size > 1){
      for (i <- 1 to relation.size){
        val subarrays = selectIelementes(i,relation).filter(p => _subsetOrder(prevSelected,p))

        for (j <- subarrays){

          val remainRelations = relation.diff(j)
          val subsets = _enumerateSet(j, remainRelations)

          if (subsets.size != 0){
            for (k <- subsets){
              res += j +: k
            }
          } else if (j.size == relation.size){
            res += ArrayBuffer(j)
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
