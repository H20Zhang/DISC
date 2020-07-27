package org.apache.spark.disc.optimization.cost_based.comp

import org.apache.spark.disc.catlog.Catalog.AttributeID
import org.apache.spark.disc.catlog.Schema
import org.apache.spark.disc.optimization.cost_based.stat.Statistic
import org.apache.spark.disc.util.misc.Conf

import scala.collection.mutable.ArrayBuffer
//
class EnumShareComputer(schemas: Seq[Schema],
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

    var maximalLoad = Conf.defaultConf().HCUBE_MEMORY_BUDGET

    while (optimalLoad > maximalLoad) {
      numTask = (numTask * 2).toInt
      Conf.defaultConf().NUM_PARTITION = numTask
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
      val load = (communicationCost.toDouble / share.product) * 10 //10 Bytes per edge

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

class NonLinearShareComputer(schemas: Seq[Schema],
                             cardinalities: Seq[Double],
                             memoryBudget: Double) {

  val attrIds = schemas.flatMap(_.attrIDs).distinct
  val attrIdsWithIdx = attrIds.zipWithIndex.map(f => (f._1, f._2 + 1))

  def optimalShare(): Map[AttributeID, Int] = {
    val script = genOctaveScript()

//    println(s"octaveScript:${script}")
    val rawShare = performOptimization(script)
    val share = roundOctaveResult(rawShare)
    share
  }

  def commCost(share: Map[AttributeID, Int]): Double = {
    val cost = schemas
      .zip(cardinalities)
      .map {
        case (schema, cardinality) =>
          val notIncludedAttrId = attrIds.filter { attrId =>
            !schema.attrIDs.contains(attrId)
          }
          val ratio = notIncludedAttrId.map { attrId =>
            share(attrId)
          }.product

          cardinality * ratio
      }
      .sum

//    cost
    if ((cost / share.values.product) < memoryBudget) {
      cost
    } else {

//      println(s"invalidShare:${share} with cost:${cost}")

//      println(
//        s"schemas:${schemas}, cardinality:${cardinalities}, problematic shareMap:${share}, budget:${memoryBudget}, estimate load:${(cost / share.values.product)}"
//      )

      Double.MaxValue
//      cost
    }
  }

  def genOctaveScript(): String = {

    val factor = 1000
    val objScript = schemas
      .zip(cardinalities)
      .map {
        case (schema, cardinality) =>
          val notIncludedAttrWithIdx = attrIdsWithIdx.filter {
            case (attrId, idx) => !schema.attrIDs.contains(attrId)
          }
          val ratioScript = notIncludedAttrWithIdx
            .map { case (attrId, idx) => s"x(${idx})*" }
            .reduce(_ + _)
            .dropRight(1)
          val relationCostScript = s"${cardinality / factor}*${ratioScript}"
          relationCostScript
      }
      .map(relationCostScript => s"${relationCostScript}+")
      .reduce(_ + _)
      .dropRight(1)

    val memoryConstraintScript = s"${memoryBudget} - (" + schemas
      .map { schema =>
        val ratio = schema.attrIDs
          .map(attrIds.indexOf)
          .map(idx => s"x(${idx + 1})*")
          .reduce(_ + _)
          .dropRight(1)
        val cardinality = cardinalities(schemas.indexOf(schema))
        s"${cardinality}/(${ratio})+"
      }
      .reduce(_ + _)
      .dropRight(1) + ");"

    val lowerBoundScript = attrIdsWithIdx
      .map { case (_, idx) => s"1;" }
      .reduce(_ + _)
      .dropRight(1)

    val upperBoundScript = attrIdsWithIdx
      .map {
        case (attrId, idx) =>
          if (schemas
                .filter(schema => schema.attrIDs.contains(attrId))
                .size == 1) {
            s"1;"
          } else {
            s"1000;"
          }
      }
      .reduce(_ + _)
      .dropRight(1)

    val initialPointScript = attrIdsWithIdx
      .map {
        case (_, idx) =>
          s"${Math.pow(Conf.defaultConf().NUM_MACHINE, 1.0 / attrIds.size)};"
      }
      .reduce(_ + _)
      .dropRight(1)

    val machineNumConstraintScript = attrIdsWithIdx
      .map { case (_, idx) => s"x(${idx})*" }
      .reduce(_ + _)
      .dropRight(1) + s" - ${Conf.defaultConf().NUM_MACHINE};"

    val octaveScript =
      s"""
         |#!octave -qf
         |1;
         |
         |function r = h (x)
         |  r = [${memoryConstraintScript} ${machineNumConstraintScript}];
         |endfunction
         |
         |function obj = phi (x)
         |  obj = $objScript;
         |endfunction
         |
         |x0 = [${initialPointScript}];
         |lower = [${lowerBoundScript}];
         |upper = [${upperBoundScript}];
         |
         |[x, obj, info, iter, nf, lambda] = sqp (x0, @phi, [], @h,lower,upper,500);
         |
         |disp(x')
         |""".stripMargin

    octaveScript
  }

  def performOptimization(script: String): Map[AttributeID, Double] = {

    import java.io._

    val tempFile = new File("./tempFile.m")
    val pw = new PrintWriter(tempFile)
    pw.write(script)
    pw.close

//    println(s"octave script:${script}")

    import sys.process._
    val result = "octave -qf ./tempFile.m" !!

    tempFile.delete()

    var rawShareVector = result.split("\\s").filter(_.nonEmpty).map(_.toDouble)

    //fail safe, there is some bug in octave
    if (rawShareVector.isEmpty) {
      rawShareVector = attrIds.map(id => Double.MaxValue / 2).toArray
    }

    val rawShare = attrIds.zip(rawShareVector).toMap

    rawShare
  }

  def roundOctaveResult(
    shareMap: Map[AttributeID, Double]
  ): Map[AttributeID, Int] = {

    val shareSeq = shareMap.toSeq
    val shareSize = shareMap.values.size
    var roundUpOrDowns = ArrayBuffer(Array(true), Array(false))
    Range(1, shareSize).foreach { idx =>
      roundUpOrDowns = roundUpOrDowns.flatMap { roundUpOrDown =>
        Array(true, false).map(f => roundUpOrDown :+ f)
      }
    }

    var roundedShareMaps = roundUpOrDowns.map { f =>
      f.zipWithIndex.map {
        case (roundUpDecision, idx) =>
          var (attrId, shareForAttrId) = shareSeq(idx)
          if (roundUpDecision) {
            shareForAttrId = math.floor(shareForAttrId) + 1
          } else {
            shareForAttrId = math.floor(shareForAttrId)
          }

          (attrId, shareForAttrId.toInt)
      }.toMap
    }

    roundedShareMaps = roundedShareMaps.filter { share =>
      share.values.product > Conf.defaultConf().NUM_MACHINE
    }

    val optimalRoundedShareMap = roundedShareMaps
      .map(shareMap => (shareMap, commCost(shareMap)))
      .sortBy(_._2)
      .head
      ._1

    optimalRoundedShareMap
  }

}
