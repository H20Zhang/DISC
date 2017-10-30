package org.apache.spark.Logo.dataStructure

import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

abstract class LogoBlockMaker[A:ClassTag,B:ClassTag](val sc:SparkContext, val colorNums:Int, val partitioner:Partitioner) {

  require(colorNums == partitioner.numPartitions, "color of Blocks must be the same with partitioner.numPartitions")

  def makeBlocks(rdd:RDD[(A,B)]):RDD[LogoBlock]
  def generateSentry(rdd:RDD[(A,B)]):RDD[(A,B)]
}

class TwoTupleOneHoleListLogoBlockMaker(sc:SparkContext,  partitioner: Partitioner)
  extends LogoBlockMaker[VertexId,VertexId](sc, partitioner.numPartitions ,partitioner){

  override def makeBlocks(rdd: RDD[(VertexId, VertexId)]) = {
    val sentriedRDD = generateSentry(rdd)
    sentriedRDD.partitionBy(partitioner).mapPartitionsWithIndex[LogoBlock](
      { (x,y) =>
        Iterator(new TwoTupleOneHoleListLogoBlock(x,y.filter(_ != (x,-x)).toList))
      },true
    )
  }

  override def generateSentry(rdd: RDD[(VertexId, VertexId)]) = {
    val sentries = sc.parallelize(Range(0,colorNums)).map(f => (f.toLong,(-f).toLong))
    rdd ++ sentries
  }
}

class TwoTupleOneHoleListLogoBlockHashMaker(sc:SparkContext, colorNums:Int)
  extends TwoTupleOneHoleListLogoBlockMaker(sc,new HashPartitioner(colorNums))



class TwoTupleTwoHoleListLogoBlockMaker(sc:SparkContext,  partitioner: HashPartitioner2)
  extends LogoBlockMaker[(VertexId,VertexId),Boolean](sc, partitioner.numPartitions ,partitioner){

  override def makeBlocks(rdd: RDD[((VertexId, VertexId),Boolean)]) = {
    val sentriedRDD = generateSentry(rdd)
    val row = partitioner.p1
    val col = partitioner.p2
    sentriedRDD.partitionBy(partitioner).mapPartitionsWithIndex[LogoBlock](
      { (x,y) =>
        Iterator(new TwoTupleTwoHoleListLogoBlock((x/row,x%col),y.filter(_._2 != false).map(_._1).toList))
      },true
    )
  }

  override def generateSentry(rdd: RDD[((VertexId, VertexId),Boolean)]) = {

    implicit class Crossable[X](xs: Traversable[X]) {
      def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
    }

    val row = partitioner.p1
    val col = partitioner.p2

    val sentries = sc.parallelize((Range(0,row) cross Range(0,col)).toList).map(f => ((f._1.toLong,f._2.toLong),false))
    rdd ++ sentries
  }
}

class TwoTupleTwoHoleListLogoBlockHashMaker(sc:SparkContext, color1:Int, color2:Int)
  extends TwoTupleTwoHoleListLogoBlockMaker(sc,new HashPartitioner2(color1,color2))











