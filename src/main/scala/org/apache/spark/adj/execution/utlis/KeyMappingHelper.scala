package org.apache.spark.adj.execution.utlis


//The keyMapping here is a OneToOne reversable mapping
object KeyMappingHelper {

  def getReverseKeyMapping(keyMapping: Seq[Int]) = {
    keyMapping.zipWithIndex.toMap
  }
}
