package org.apache.spark.adj.deprecated.utlis


//The keyMapping here is a OneToOne reversable mapping
object KeyMappingHelper {

  def getReverseKeyMapping(keyMapping: Seq[Int]) = {
    keyMapping.zipWithIndex.toMap
  }
}
