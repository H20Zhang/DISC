package org.apache.spark.adj.optimization.comp

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.optimization.stat.Statistic
import org.apache.spark.adj.utils.misc.Conf
import org.apache.spark.adj.utils.misc.Conf.Method

import scala.collection.mutable.ArrayBuffer
//
class EnumShareComputer(schemas: Seq[RelationSchema],
                        tasks: Int,
                        statistic: Statistic = Statistic.defaultStatistic()) {

  var numTask = tasks
  val attrIds = schemas.flatMap(_.attrIDs).distinct
  val cardinalities =
    schemas.map(schema => (schema, statistic.cardinality(schema)))

  def genAllShare() = {
    //    get all shares
    val shareEnumerator = new ShareEnumerator(attrIds, numTask)
    val allShare = shareEnumerator.genAllShares()
    allShare
  }

  def optimalShare() = {

    var optimalShareAndCost = optimalShareAndLoadAndCost()
    var optimalShare = optimalShareAndCost._1
    var optimalLoad = optimalShareAndCost._3

    var maximalLoad = 0.0

    Conf.defaultConf().method match {
      case Method.MergedHCube => maximalLoad = 5 * Math.pow(10, 7)
      case _                  => maximalLoad = 3 * Math.pow(10, 7)
    }

    while (optimalLoad > maximalLoad) {
      numTask = (numTask * 2).toInt
      Conf.defaultConf().taskNum = numTask
      optimalShareAndCost = optimalShareAndLoadAndCost()
      optimalShare = optimalShareAndCost._1
      optimalLoad = optimalShareAndCost._3
    }

    optimalShareAndCost
  }

  def optimalShareAndLoadAndCost() = {

    //    get all shares
    val allShare = genAllShare()

    //    find optimal share --- init
    val attrIdsToPos = attrIds.zipWithIndex.toMap
    var minShare: Array[Int] = Array()
    var minCommunication: Long = Long.MaxValue
    var minLoad: Double = Double.MaxValue
    var shareSum: Int = Int.MaxValue

    val excludedAttributesOfRelationAndCardinality = cardinalities
      .map(f => (attrIds.filter(A => !f._1.attrIDs.contains(A)), f._2))
      .map(f => (f._1.map(attrIdsToPos), f._2))

    //    find optimal share --- examine communication cost incurred by every share
    allShare.foreach { share =>
      val communicationCost = excludedAttributesOfRelationAndCardinality.map {
        case (excludedAttrs, cardinality) =>
          var multiplyFactor = 1l

          excludedAttrs.foreach {
            case idx =>
              multiplyFactor = multiplyFactor * share(idx)
          }

          multiplyFactor * cardinality
      }.sum

//      val totalTask = share.product
      val load = communicationCost.toDouble / share.product

      if (load == minLoad && share.sum < shareSum) {
        minLoad = load
        minCommunication = communicationCost
        minShare = share
        shareSum = share.sum
      } else if (load < minLoad) {
        minLoad = load
        minCommunication = communicationCost
        minShare = share
        shareSum = share.sum
      }

    }

    (attrIdsToPos.mapValues(idx => minShare(idx)), minCommunication, minLoad)
  }

}

/**
  * @param PatternSize: Array((Array:The Attribute Pattern has using number representation,Int:Size of the Pattern))
  * @param minP: min amount of subtasks generated
  * @param maxP: max amount of subtasks generated
  * @param length: number of attributes
  */
class ShareEnumerator(attributes: Seq[AttributeID], tasks: Int) {

  //  val pGenerator = new PGenerator(maxP,length, p = { f => f.product > minP})

  val length = attributes.size

  def genAllShares(): ArrayBuffer[Array[Int]] = {
    _genAllShare(1, length)
  }

  private def _genAllShare(prevProd: Int,
                           remainLength: Int): ArrayBuffer[Array[Int]] = {

    val largest_possible = tasks / prevProd

    if (remainLength == 1) {
      val mutableArray = new ArrayBuffer[Array[Int]]()
      for (i <- 1 to largest_possible) {
        mutableArray += Array(i)
      }

      return mutableArray
    } else {
      val mutableArray = new ArrayBuffer[Array[Int]]()
      for (i <- 1 to largest_possible) {
        val subs = _genAllShare(prevProd * i, remainLength - 1)
        for (j <- subs) {
          val tempArray = new Array[Int](remainLength)
          j.copyToArray(tempArray)
          tempArray(remainLength - 1) = i
          mutableArray += tempArray
        }
      }
      return mutableArray
    }
  }
}

//TODO: finish it

//memoryBudget(GB)
class NonLinearShareComputer(
  schemas: Seq[RelationSchema],
  memoryBudget: Double,
  statistic: Statistic = Statistic.defaultStatistic()
) {

  val attrIds = schemas.flatMap(_.attrIDs).distinct
  val cardinalities =
    schemas.map(schema => (schema, statistic.cardinality(schema)))

  def optimalShare(): Map[AttributeID, Int] = ???

  def genOctaveScript() = ???

  def parseOctaveResult() = ???

  def roundOctaveResult() = ???

}
