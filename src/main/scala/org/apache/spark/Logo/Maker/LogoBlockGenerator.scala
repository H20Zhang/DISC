package org.apache.spark.Logo.Maker

import org.apache.spark.Logo.dataStructure.{LogoMetaData, LogoSchema, RowLogoBlock}
import org.apache.spark.Logo.utlis.PointToNumConverter
import org.apache.spark.Partitioner
import spire.ClassTag

abstract class LogoBlockGenerator[A:ClassTag, B:ClassTag](val schema: LogoSchema, val index:Int, val data:Iterator[A]) {
  val baseList = schema.slotSize
  val converter = new PointToNumConverter(baseList)
  val numList = converter.NumToList(index)
  val numParts = data.length

  val metaData = LogoMetaData(numList,schema,numParts)

  def generate():B
}

class rowBlockGenerator[A:ClassTag](schema: LogoSchema,
                                    index:Int,
                                    data:Iterator[(List[Int],A)]) extends LogoBlockGenerator[(List[Int],A),RowLogoBlock[(List[Int],A)]](schema,index,data) {
  override def generate() = {
    val rowBlock = new RowLogoBlock[(List[Int],A)](schema,metaData,data.toList)
    rowBlock
  }
}




