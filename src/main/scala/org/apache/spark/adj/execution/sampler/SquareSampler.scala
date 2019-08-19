package org.apache.spark.adj.execution.sampler

import breeze.linalg.sum
import org.apache.spark.HashPartitioner
import org.apache.spark.adj.execution.rdd.loader.DataLoader
import org.apache.spark.adj.execution.utlis.SparkSingle

import scala.collection.mutable.ArrayBuffer

class SquareSampler(data:String) {

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


  lazy val degree ={
    val seqOp = (d:Int, id:Int) => d + 1
    val combineOp = (d1:Int, d2:Int) => d1 + d2
    val degree = edge.aggregateByKey(0)(seqOp, combineOp).collectAsMap()
    val sc = SparkSingle.getSparkContext()
    val broadcastDegree = sc.broadcast(degree)
    broadcastDegree
  }

//  lazy val point = degree.map(_._1)
  lazy val weightedEdge = {
    val localDegree = degree
    val temp = edge.mapPartitions{g =>
      val paritionDegree = localDegree.value
      g.map{f => (f,(paritionDegree(f._2) - 1).toLong* (paritionDegree(f._2)-1))}
    }.repartition(400).cache()
    temp.count()
    temp
  }

  def selectEdges(k:Int) = {

    edge.count()

    val parts = weightedEdge.getNumPartitions


    //    Collect Total Degree
    val pointPartitionInfo = weightedEdge.mapPartitionsWithIndex{
      case(id, it) =>
        val aggOp = (temp: Long, f:((Int,Int), Long)) => temp + f._2
        val partitionTotalDegree = it.foldLeft(0L)(aggOp)
        Iterator((id,partitionTotalDegree))
    }.collect().sortBy(_._1)

    val aggOp = (temp: Long, f:(Int, Long)) => temp + f._2
    val totalDegree = pointPartitionInfo.foldLeft(0L)(aggOp)

    //    Generate k sample according to round robin.
    //    Generate the pos of the sample to retrieve locally.
    val r = scala.util.Random

    //    print(s"total degree is :${totalDegree}")


    val SamplesPos = SparkSingle.getSparkContext().parallelize(Range(0, parts), parts).mapPartitions(f => Range(0, k/parts).iterator)
      .mapPartitions(f =>  f.map(_ => Math.abs(r.nextLong()) % totalDegree))
      .map{f =>

        val size = pointPartitionInfo.size
        var i = 0
        var posSum = 0L
        var result:(Int, Long) = null
        while (i < size){
          if (((posSum + (pointPartitionInfo(i)._2)) > f)){
            result = (i, f-posSum)
            i += size
          } else {
            posSum = posSum + (pointPartitionInfo(i)._2)
            i += 1
          }


        }
        result
      }.partitionBy(new HashPartitioner(parts))

    //    There maybe some bugs when zipping the partitions
    val rddSamples = weightedEdge.zipPartitions(SamplesPos){
      case (it,dictIt) =>
        val buffer = ArrayBuffer[(Int,Int)]()
        val samplePos = dictIt.toArray.map(_._2).sorted
        val samplePosSize = samplePos.size
        var idx = 0
        val scanOp = {(sum:Long, g:((Int,Int),Long)) =>
          while (idx < samplePosSize && samplePos(idx) < sum + g._2){
            buffer += g._1
            idx += 1
          }
          sum + g._2
        }

        it.foldLeft(0L)(scanOp)

        buffer.toIterator
    }



    val samples = rddSamples.cache()

    samples.count()

//    val spark = SparkSingle.getSparkSession()
//    import spark.implicits._

//    samples.toDS().map(f => f._1).distinct().map(f => (f,1)).


    samples
  }

  def threePathSample(k:Int) = {
    val edgeSamples = selectEdges(k).cache()
    edgeSamples.count()

    val groupedEdges = edge.groupByKey(new HashPartitioner(200)).cache()
    groupedEdges.count()

    val lEdges = groupedEdges.mapPartitions{
      f =>
        val r = scala.util.Random
        r.setSeed(System.currentTimeMillis())
        f.map{g =>
          val edges = g._2.toArray
          val size = edges.size
          (g._1,edges(r.nextInt(size)))
        }
    }.cache()

    val rEdges = groupedEdges.mapPartitions{
      f =>
        val r = scala.util.Random
        r.setSeed(System.currentTimeMillis())
        f.map{g =>
          val edges = g._2.toArray
          val size = edges.size
          (g._1,edges(r.nextInt(size)))
        }
    }.cache()

    val spark = SparkSingle.getSparkSession()
    import spark.implicits._


    edgeSamples.toDF("lNode", "rNode").createOrReplaceTempView("C")
    lEdges.toDF("lNode", "extendl").createOrReplaceTempView("L")
    rEdges.toDF("rNode", "extendr").createOrReplaceTempView("R")

//    val filterEdges = edgeDataset.toDF("src", "dst").cache()
//    filterEdges.createOrReplaceGlobalTempView("Edge")

    val query =
      """
        |select extendl as l, C.lNode as c1, C.rNode
        |from C, L
        |where C.lNode = L.lNode
      """.stripMargin

    val twoPaths = spark.sql(query).createOrReplaceTempView("TwoPath")

    val query2 =
      """
        |select l, c1, T.rNode as c2, extendr as r
        |from TwoPath as T, R
        |where T.rNode = R.rNode
      """.stripMargin

    val threePaths = spark.sql(query2)

    threePaths.cache()
  }



  def squareSamplesCount(k:Int) = {
    val spark = SparkSingle.getSparkSession()
    import spark.implicits._
    val threePaths = threePathSample(k)
    threePaths.createOrReplaceTempView("ThreePath")

    val filterEdges = edgeDataset.toDF("src", "dst").cache()
    filterEdges.createOrReplaceTempView("Edge")

    val query =
      """
        |select T.c1, T.c2, T.l, T.r
        |from ThreePath as T, Edge as E
        |where T.l = E.src and T.r = E.dst
      """.stripMargin

    val result = spark.sql(query)
    result.count()
  }


  def threePathSampleForFourClique(k:Int) = {
    val edgeSamples = selectEdges(k).cache()
    edgeSamples.count()

    val groupedEdges = edge.groupByKey(new HashPartitioner(200)).cache()
    groupedEdges.count()

    val lEdges = groupedEdges.mapPartitions{
      f =>
        val r = scala.util.Random
        r.setSeed(System.currentTimeMillis())
        f.map{g =>
          val edges = g._2.toArray
          val size = edges.size
          (g._1,edges(r.nextInt(size)))
        }
    }

    val rEdges = groupedEdges.mapPartitions{
      f =>
        val r = scala.util.Random
        r.setSeed(System.currentTimeMillis())
        f.map{g =>
          val edges = g._2.toArray
          val size = edges.size
          (g._1,edges(r.nextInt(size)))
        }
    }

    val spark = SparkSingle.getSparkSession()
    import spark.implicits._


    edgeSamples.toDF("lNode", "rNode").createOrReplaceTempView("C")
    lEdges.toDF("lNode", "extendl").createOrReplaceTempView("L")
    rEdges.toDF("rNode", "extendr").createOrReplaceTempView("R")

    val filterEdges = edgeDataset.toDF("src", "dst").cache()
    filterEdges.createOrReplaceTempView("Edge")

    val query =
      """
        |select extendl as l, C.lNode as c1, C.rNode
        |from C, L, Edge as E
        |where C.lNode = L.lNode and E.src = extendl and E.dst = C.rNode
      """.stripMargin

    val twoPaths = spark.sql(query).createOrReplaceTempView("TwoPath")

    val query2 =
      """
        |select  l, c1, T.rNode as c2, extendr as r
        |from TwoPath as T, R
        |where T.rNode = R.rNode
      """.stripMargin

    val threePaths = spark.sql(query2)

    threePaths
  }

  def fourCliqueSamplesCount(k:Int) = {
    val spark = SparkSingle.getSparkSession()
    import spark.implicits._
    val threePaths = threePathSampleForFourClique(k)
    threePaths.createOrReplaceTempView("ThreePath")

    val filterEdges = edgeDataset.toDF("src", "dst").cache()
    filterEdges.createOrReplaceTempView("Edge")

    val query =
      """
        |select  T.c1, T.c2, T.l, T.r
        |from ThreePath as T, Edge as E1, Edge as E3
        |where T.l = E1.src and T.r = E1.dst and T.r = E3.src and T.c1 = E3.dst
      """.stripMargin

    val result = spark.sql(query)
    result.count()
  }
}
