package org.apache.spark.adj.deprecated.execution.sampler

import breeze.linalg.sum
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.adj.deprecated.execution.rdd.loader.DataLoader
import org.apache.spark.adj.utils.misc.SparkSingle
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer

class TriangleSampler(data: String) {

  lazy val edgeDataset = {
    val dataset = new DataLoader(data).EdgeDataset.cache()
    dataset.count()
    dataset
  }

  lazy val edge = {
    val temp = edgeDataset.rdd.cache()
    edgeDataset.count()
    temp
  }

  lazy val degree = {
    val seqOp = (d: Int, id: Int) => d + 1
    val combineOp = (d1: Int, d2: Int) => d1 + d2
    val degree = edge.aggregateByKey(0)(seqOp, combineOp)
    degree
  }

  lazy val point = degree.map(_._1)
  lazy val weightedPoint = {
    val temp = degree.map(f => (f._1, f._2.toLong * (f._2 - 1))).cache()
    temp.count()
    temp
  }

  def selectPoints(k: Int) = {

    edge.count()

//    Collect Total Degree
    val pointPartitionInfo = weightedPoint
      .mapPartitionsWithIndex {
        case (id, it) =>
          val aggOp = (temp: Long, f: (Int, Long)) => temp + f._2
          val partitionTotalDegree = it.foldLeft(0L)(aggOp)
          Iterator((id, partitionTotalDegree))
      }
      .collect()
      .sortBy(_._1)

    val aggOp = (temp: Long, f: (Int, Long)) => temp + f._2
    val totalDegree = pointPartitionInfo.foldLeft(0L)(aggOp)

//    Generate k sample according to round robin.
//    Generate the pos of the sample to retrieve locally.
    val r = scala.util.Random

//    print(s"total degree is :${totalDegree}")

    val SamplesPos = SparkSingle
      .getSparkContext()
      .parallelize(Range(0, 200), 200)
      .mapPartitions(f => Range(0, k / 200).iterator)
      .mapPartitions(f => f.map(_ => Math.abs(r.nextLong()) % totalDegree))
      .map { f =>
        val size = pointPartitionInfo.size
        var i = 0
        var posSum = 0L
        var result: (Int, Long) = null
        while (i < size) {
          if (((posSum + (pointPartitionInfo(i)._2)) > f)) {
            result = (i, f - posSum)
            i += size
          } else {
            posSum = posSum + (pointPartitionInfo(i)._2)
            i += 1
          }

        }
        result
      }
      .partitionBy(new HashPartitioner(200))

//    There maybe some bugs when zipping the partitions
    val rddSamples = weightedPoint.zipPartitions(SamplesPos) {
      case (it, dictIt) =>
        val buffer = ArrayBuffer[Int]()
        val samplePos = dictIt.toArray.map(_._2).sorted
        val samplePosSize = samplePos.size
        var idx = 0
        val scanOp = { (sum: Long, g: (Int, Long)) =>
          while (idx < samplePosSize && samplePos(idx) < sum + g._2) {
            buffer += g._1
            idx += 1
          }
          sum + g._2
        }

        it.foldLeft(0L)(scanOp)

        buffer.toIterator
    }

    val seqOp1 = (d: Int, id: Int) => d + 1
    val combineOp1 = (d1: Int, d2: Int) => d1 + d2
    val samples = rddSamples
      .map(f => (f, 1))
      .aggregateByKey(0)(seqOp1, combineOp1)
      .partitionBy(new HashPartitioner(200))

//    val samples = rddSamples.groupBy(f => f).map(f => (f._1, f._2.size)).partitionBy(new HashPartitioner(200))
    samples
  }

  def wedgeSample(k: Int) = {
    val pointSamples = selectPoints(k)
    val groupedEdges =
      edge.groupByKey(new HashPartitioner(200)).zipPartitions(pointSamples) {
        case (nodesIt, sampleIt) =>
          val pointSamplesDict = sampleIt.toMap
          val r = scala.util.Random
          val partitionSamples = nodesIt.flatMap { g =>
            if (pointSamplesDict.contains(g._1)) {
              val gEdges = g._2.toArray
              val size = gEdges.size
              val gSamples = new Array[(Int, Int)](pointSamplesDict(g._1))

              gSamples.map { f =>
                val lId = r.nextInt(size)
                val rId = r.nextInt(size)

                if (lId != rId) {
                  (g._1, gEdges(lId), gEdges(rId))
                } else {
                  (g._1, gEdges(0), gEdges(1))
                }

              }
            } else {
              Iterator()
            }
          }
          partitionSamples
      }

    groupedEdges
  }

  def triangleSamplesCount(k: Int) = {
    val spark = SparkSingle.getSparkSession()
    import spark.implicits._
    val wedges = wedgeSample(k).toDF("center", "src", "dst").cache()
    val filterEdges = edgeDataset.toDF("src", "dst").cache()

    wedges.createOrReplaceTempView("Wedge")
    filterEdges.createOrReplaceTempView("Edge")

    val query =
      """
        |select center, W.src, W.dst
        |from Wedge as W, Edge as E
        |where W.src = E.src and W.dst = E.dst
      """.stripMargin

    val result = spark.sql(query)
    result.count()
  }
}
