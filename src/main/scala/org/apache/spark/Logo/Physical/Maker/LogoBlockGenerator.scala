package org.apache.spark.Logo.Physical.Maker

import org.apache.spark.Logo.Physical.dataStructure.{LogoMetaData, LogoSchema, RowLogoBlock}
import org.apache.spark.Logo.Physical.utlis.PointToNumConverter
import org.apache.spark.Partitioner
import spire.ClassTag

abstract class LogoBlockGenerator[A:ClassTag, B:ClassTag](val schema: LogoSchema, val index:Int, val data:Iterator[A]) {
  lazy val baseList = schema.slotSize
  lazy val numList =schema.IndexToKey(index)
  lazy val numParts = filteredData.length
  var filteredData:List[A] = _

  lazy val metaData = LogoMetaData(numList,schema,numParts)

  def generate():B
}

class rowBlockGenerator[A:ClassTag](schema: LogoSchema,
                                    index:Int,
                                    data:Iterator[(List[Int],A)]) extends LogoBlockGenerator[(List[Int],A),RowLogoBlock[(List[Int],A)]](schema,index,data) {


  filteredData = data.filter(_._2 != null).toList
////  filteredData.toList.foreach(println)
//
//  println("hello")
//  println(filteredData.toList)
//  println(schema)

  override def generate() = {
    val rowBlock = new RowLogoBlock[(List[Int],A)](schema,metaData,filteredData)
    rowBlock
  }
}




