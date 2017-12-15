package TestData

import org.apache.spark.Logo.Physical.dataStructure._

object TestPatternBlockData {
  lazy val edgeBlock = {
    val schema = LogoSchema(KeyMapping(Seq(3,3)))
    val metaData = LogoMetaData(Seq(1,2),10)
    val rawData = List.fill(10)(PatternInstance(Seq(1,2)))

    new EdgePatternLogoBlock(schema,metaData,rawData)
  }
}
