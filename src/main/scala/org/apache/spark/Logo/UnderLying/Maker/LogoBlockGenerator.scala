package org.apache.spark.Logo.UnderLying.Maker

import org.apache.spark.Logo.UnderLying.dataStructure._
import spire.ClassTag

abstract class LogoBlockGenerator[A: ClassTag, B: ClassTag](val schema: LogoSchema, val index: Int, val data: Iterator[A]) {


  def generate(): B
}


class rowBlockGenerator[A: ClassTag](schema: LogoSchema,
                                     index: Int,
                                     data: Iterator[(Seq[Int], A)]) extends LogoBlockGenerator[(Seq[Int], A), RowLogoBlock[(Seq[Int], A)]](schema, index, data) {
  lazy val baseList = schema.slotSize
  lazy val numList = schema.IndexToKey(index)
  var filteredData = data.filter(_._2 != null).toBuffer
  lazy val numParts = filteredData.length
  lazy val metaData = LogoMetaData(numList, numParts)


  override def generate() = {
    val rowBlock = new RowLogoBlock[(Seq[Int], A)](schema, metaData, filteredData)
    rowBlock
  }
}

class CompactRowGenerator(schema: LogoSchema,
                          index: Int,
                          data: Iterator[((Int, Int), Int)]) extends LogoBlockGenerator[((Int, Int), Int), CompactConcretePatternLogoBlock](schema, index, data) {

  lazy val baseList = schema.slotSize
  lazy val numList = schema.IndexToKey(index)
  var filteredData = data.filter(_._2 != null)
  lazy val numParts = filteredData.length
  lazy val metaData = LogoMetaData(numList, numParts)


  override def generate(): CompactConcretePatternLogoBlock = {

    val rawData = new CompactListAppendBuilder(schema.keyCol.size)
    data.foreach {
      f =>
        rawData.append(f._1._1)
        rawData.append(f._1._2)
    }

    val compactBlock = new CompactConcretePatternLogoBlock(schema, metaData, rawData.toCompactList())

    compactBlock

  }
}






