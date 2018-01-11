package org.apache.spark.Logo.UnderLying.Maker

import org.apache.spark.Logo.UnderLying.dataStructure.{LogoMetaData, LogoSchema, RowLogoBlock}
import org.apache.spark.Logo.UnderLying.utlis.PointToNumConverter
import org.apache.spark.Partitioner
import spire.ClassTag

abstract class LogoBlockGenerator[A:ClassTag, B:ClassTag](val schema: LogoSchema, val index:Int, val data:Iterator[A]) {
  lazy val baseList = schema.slotSize
  lazy val numList =schema.IndexToKey(index)
  lazy val numParts = filteredData.length
  var filteredData:List[A] = _

  lazy val metaData = LogoMetaData(numList,numParts)

  def generate():B
}


class rowBlockGenerator[A:ClassTag](schema: LogoSchema,
                                    index:Int,
                                    data:Iterator[(Seq[Int],A)]) extends LogoBlockGenerator[(Seq[Int],A),RowLogoBlock[(Seq[Int],A)]](schema,index,data) {


  filteredData = data.filter(_._2 != null).toList

  override def generate() = {
    val rowBlock = new RowLogoBlock[(Seq[Int],A)](schema,metaData,filteredData)
    rowBlock
  }
}




