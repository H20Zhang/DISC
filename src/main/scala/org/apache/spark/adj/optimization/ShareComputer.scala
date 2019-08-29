package org.apache.spark.adj.optimization

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.optimization.utils.Statistic
import org.apache.spark.adj.plan.Join

import scala.collection.mutable.ArrayBuffer
//
class ShareComputer(schemas:Seq[RelationSchema], tasks:Int, statistic:Statistic = Statistic.defaultStatistic()) {

    val attrIds = schemas.flatMap(_.attrIDs).distinct
    val cardinalities = schemas.map(schema => (schema, statistic.cardinality(schema)))

    def optimalShare():Map[AttributeID, Int] = {

  //    get all shares
      val shareEnumerator = new ShareEnumerator(attrIds, tasks)
      val allShare = shareEnumerator.genAllShares()

  //    find optimal share --- init
      val attrIdsToPos = attrIds.zipWithIndex.toMap
      var minShare:Array[Int] = Array()
      val minCommunication:Long = Long.MaxValue
      val minLoad:Long = Long.MaxValue


      val excludedAttributesOfRelationAndCardinality = cardinalities
        .map(f => (attrIds.filter(A => ! f._1.attrIDs.contains(A)), f._2))
        .map(f => (f._1.map(attrIdsToPos), f._2))

  //    find optimal share --- examine communication cost incurred by every share
      allShare.foreach{
        share =>
          val communicationCost = excludedAttributesOfRelationAndCardinality.map{
            case (excludedAttrs, cardinality) =>

              var multiplyFactor = 0l

              excludedAttributesOfRelationAndCardinality.foreach{
                case (attrIdxs, cardiality) =>
                  attrIdxs.foreach{
                    idx => multiplyFactor = multiplyFactor * share(idx)
                  }
              }

              multiplyFactor * cardinality
          }.sum

          if (communicationCost < minCommunication){
            minShare = share
          }
      }


      attrIdsToPos.mapValues(idx => minShare(idx))
    }

}


/**
  * @param PatternSize: Array((Array:The Attribute Pattern has using number representation,Int:Size of the Pattern))
  * @param minP: min amount of subtasks generated
  * @param maxP: max amount of subtasks generated
  * @param length: number of attributes
  */
class ShareEnumerator(attributes:Seq[AttributeID], tasks:Int) {

  //  val pGenerator = new PGenerator(maxP,length, p = { f => f.product > minP})

  val length = attributes.size

  def genAllShares():ArrayBuffer[Array[Int]] = {
    _genAllShare(1,length)
  }

  private def _genAllShare(prevProd:Int,remainLength:Int):ArrayBuffer[Array[Int]] = {

    val largest_possible = tasks / prevProd

    if (remainLength == 1) {
      val mutableArray = new ArrayBuffer[Array[Int]]()
      for (i <- 1 to largest_possible){
        mutableArray += Array(i)
      }

      return mutableArray
    } else {
      val mutableArray = new ArrayBuffer[Array[Int]]()
      for (i <- 1 to largest_possible){
        val subs = _genAllShare(prevProd * i, remainLength - 1)
        for (j <- subs) {
          val tempArray = new Array[Int](remainLength)
          j.copyToArray(tempArray)
          tempArray(remainLength-1) = i
          mutableArray += tempArray
        }
      }
      return mutableArray
    }
  }
}

