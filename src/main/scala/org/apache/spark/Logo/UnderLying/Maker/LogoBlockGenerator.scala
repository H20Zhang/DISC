package org.apache.spark.Logo.UnderLying.Maker

import org.apache.spark.Logo.UnderLying.dataStructure._
import spire.ClassTag

abstract class LogoBlockGenerator[A: ClassTag, B: ClassTag](val schema: LogoSchema, val index: Int, val data: Iterator[A]) {


  def generate(): B
}


class rowBlockGenerator[A: ClassTag](schema: LogoSchema,
                                     index: Int,
                                     data: Iterator[(Array[Int], A)]) extends LogoBlockGenerator[(Array[Int], A), RowLogoBlock[(Array[Int], A)]](schema, index, data) {
  lazy val baseList = schema.slotSize
  lazy val numList = schema.IndexToKey(index)
  var filteredData = data.filter(_._2 != null).toArray
  lazy val numParts = filteredData.length
  lazy val metaData = LogoMetaData(numList, numParts)


  override def generate() = {
    val rowBlock = new RowLogoBlock[(Array[Int], A)](schema, metaData, filteredData)
    rowBlock
  }
}

class CompactRowGenerator[A: ClassTag](schema: LogoSchema,
                          index: Int,
                          data: Iterator[(Array[Int], A)]) extends LogoBlockGenerator[(Array[Int], A), CompactConcretePatternLogoBlock](schema, index, data) {

  lazy val baseList = schema.slotSize
  lazy val numList = schema.IndexToKey(index)
  var filteredData = data.filter(_._2 != null)
  lazy val numParts = filteredData.length
  lazy val metaData = LogoMetaData(numList, numParts)


  override def generate(): CompactConcretePatternLogoBlock = {

    val rawData = new CompactListAppendBuilder(2)
    var count = 0

    //TODO this place is very strange, this place should be understand why left and right should be reversed
    filteredData.foreach {
      f =>
        count += 1
        rawData.append(f._1(1))
        rawData.append(f._1(0))
    }

    println(s"the count is $count")
//    rawData.toCompactList().iterator().foreach{f =>
//      println()
//      print(f(0))
//      print(f(1))
//
//    }

    val compactBlock = new CompactConcretePatternLogoBlock(schema, metaData, rawData.toCompactList())

    compactBlock

  }
}






