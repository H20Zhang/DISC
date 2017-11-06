package org.apache.spark.Logo.Maker

import org.apache.spark.Logo.dataStructure._
import org.apache.spark.Logo.utlis.{ListGenerator, PointToNumConverter}
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}

import scala.reflect.ClassTag



//can only work with slotParititioner, or compositeParitioner with no more than 2 level.

abstract class RowLogoRDDMaker[A:ClassTag, B: ClassTag](val rdd: RDD[(A,B)]) {

  var _edges:List[(Int,Int)] = _
  var _keySizeMap:Map[Int,Int] = _


  lazy val schema = LogoSchema(_edges,_keySizeMap)
  lazy val _partitioner:Partitioner = schema.partitioner

  def setEdges(edges:List[(Int,Int)]) = {
    this._edges = edges
    this
  }

  def setKeySizeMap(keySizeMap:Map[Int,Int]) = {
    _keySizeMap = keySizeMap
    this
  }


  def build():RDD[RowLogoBlock[(A,B)]]
}


/**
  *
  *
  * @param rdd The RDD used to make a logoRDD
  * @param nodeSize Total slot of the Key
  * @tparam A Attribute Type
  * List[Int] Key Type
  */


class SimpleRowLogoRDDMaker[A:ClassTag](rdd:RDD[(List[Int],A)], nodeSize:Int) extends RowLogoRDDMaker(rdd){

  val sc = rdd.sparkContext
  val keyCol = _partitioner match {
    case s:SlotPartitioner => List(s.slotNum)
    case c:CompositeParitioner => c.partitioners.map(_.slotNum)
  }


  def generateSentry() = {

    var sentryNode:List[List[Int]] = null
    var sentry:List[(List[Int],A)] = null
    var sentryRDD:RDD[(List[Int],A)] = null

    _partitioner match {
      case s:SlotPartitioner => {
        val slotNum = s.slotNum
        val p1 = s.p1
        sentryNode = ListGenerator.fillListListIntoSlots(ListGenerator.cartersianList(List(p1)),nodeSize,List(slotNum))
        sentry = sentryNode.map(f => (f,null.asInstanceOf[A]))
        sentryRDD = sc.parallelize(sentry)
      }
      case c:CompositeParitioner =>{
        val slotNums = c.partitioners.map(_.slotNum)
        val baseList = c.partitioners.map(_.p1)
        sentryNode = ListGenerator.fillListListIntoSlots(ListGenerator.cartersianList(baseList),nodeSize,slotNums)
        sentry = sentryNode.map((_,null.asInstanceOf[A]))
        sentryRDD = sc.parallelize(sentry)
      }
    }

    sentryRDD
  }

  def build(): RDD[RowLogoBlock[(List[Int],A)]] ={
    val sentryRDD = generateSentry
    val baseList = _partitioner match {
      case s:SlotPartitioner => List(s.p1)
      case c:CompositeParitioner => c.partitioners.map(_.p1)
    }

    val sentriedRDD = rdd.union(sentryRDD)
    sentriedRDD.partitionBy(_partitioner)
    sentriedRDD.mapPartitionsWithIndex[RowLogoBlock[(List[Int],A)]]{case (index,f) =>
      val converter = new PointToNumConverter(baseList)
      val numList = converter.NumToList(index)
      val blockGenerator = new rowBlockGenerator(schema,index,f)
      val block = blockGenerator.generate()
      Iterator(block)
    }
  }

}





//class TwoTupleOneHoleListLogoBlockMaker(sc:SparkContext,  partitioner: Partitioner)
//  extends LogoBlockMaker[VertexId,VertexId](sc, partitioner.numPartitions ,partitioner){
//
//  override def makeBlocks(rdd: RDD[(VertexId, VertexId)]) = {
//    val sentriedRDD = generateSentry(rdd)
//    sentriedRDD.partitionBy(partitioner).mapPartitionsWithIndex[LogoBlock](
//      { (x,y) =>
//        Iterator(new TwoTupleOneHoleListLogoBlock(x,y.filter(_ != (x,-x)).toList))
//      },true
//    )
//  }
//
//  override def generateSentry(rdd: RDD[(VertexId, VertexId)]) = {
//    val sentries = sc.parallelize(Range(0,colorNums)).map(f => (f.toLong,(-f).toLong))
//    rdd ++ sentries
//  }
//}
//
//class TwoTupleOneHoleListLogoBlockHashMaker(sc:SparkContext, colorNums:Int)
//  extends TwoTupleOneHoleListLogoBlockMaker(sc,new HashPartitioner(colorNums))
//
//
//
//class TwoTupleTwoHoleListLogoBlockMaker(sc:SparkContext,  partitioner: HashPartitioner2)
//  extends LogoBlockMaker[(VertexId,VertexId),Boolean](sc, partitioner.numPartitions ,partitioner){
//
//  override def makeBlocks(rdd: RDD[((VertexId, VertexId),Boolean)]) = {
//    val sentriedRDD = generateSentry(rdd)
//    val row = partitioner.p1
//    val col = partitioner.p2
//    sentriedRDD.partitionBy(partitioner).mapPartitionsWithIndex[LogoBlock](
//      { (x,y) =>
//        Iterator(new TwoTupleTwoHoleListLogoBlock((x/row,x%col),y.filter(_._2 != false).map(_._1).toList))
//      },true
//    )
//  }
//
//  override def generateSentry(rdd: RDD[((VertexId, VertexId),Boolean)]) = {
//
//    implicit class Crossable[X](xs: Traversable[X]) {
//      def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
//    }
//
//    val row = partitioner.p1
//    val col = partitioner.p2
//
//    val sentries = sc.parallelize((Range(0,row) cross Range(0,col)).toList).map(f => ((f._1.toLong,f._2.toLong),false))
//    rdd ++ sentries
//  }
//}
//
//class TwoTupleTwoHoleListLogoBlockHashMaker(sc:SparkContext, color1:Int, color2:Int)
//  extends TwoTupleTwoHoleListLogoBlockMaker(sc,new HashPartitioner2(color1,color2))
//
//
//
//
//
//
//




