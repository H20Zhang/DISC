package org.apache.spark.adj.exp.data

import org.apache.spark.adj.execution.rdd._

object TestPatternBlockData {
  lazy val edgeBlock = {
    val schema = LogoSchema(KeyMapping(Seq(3, 3)))
    val metaData = LogoMetaData(Seq(1, 2), 10)
    val rawData = List.fill(10)(PatternInstance(Seq(1, 2)))

    new EdgePatternLogoBlock(schema, metaData, rawData)
  }


  //we assume col0 to be the key.
  lazy val keyValueEdgeBlock = {
    edgeBlock.toKeyValueLogoBlock(Set(0),false)
  }
}