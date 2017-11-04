package org.apache.spark.Logo.Maker

import org.apache.spark.Logo.dataStructure.{LogoMetaData, LogoSchema, RowLogoBlock}
import org.apache.spark.Logo.utlis.PointToNumConverter
import org.apache.spark.Partitioner
import spire.ClassTag

abstract class LogoBlockGenerator[A:ClassTag, B:ClassTag](val partitioner:Partitioner,val edge:List[(Int,Int)], val baseList:List[Int], val index:Int,val keyCol:List[Int], val nodeSize:Int, val data:Iterator[A]) {
  val converter = new PointToNumConverter(baseList)
  val numList = converter.NumToList(index)
  val numParts = data.length
  val schema = LogoSchema(partitioner,edge,keyCol, nodeSize)
  val metaData = LogoMetaData(numList,schema,numParts)

  def generate():B
}

class rowBlockGenerator[A:ClassTag](partitioner:Partitioner,
                                    edge:List[(Int,Int)],
                                    baseList:List[Int],
                                    index:Int,
                                    keyCol:List[Int],
                                    nodeSize:Int,
                                    data:Iterator[(List[Int],A)]) extends LogoBlockGenerator[(List[Int],A),RowLogoBlock[(List[Int],A)]](partitioner,edge,baseList,index ,keyCol,nodeSize,data) {
  override def generate() = {
    val rowBlock = new RowLogoBlock[(List[Int],A)](schema,metaData,data.toList)
    rowBlock
  }
}




