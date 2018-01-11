package org.apache.spark.Logo.UnderLying.utlis


//The keyMapping here is a OneToOne reversable mapping
object KeyMappingHelper {

  def getReverseKeyMapping(keyMapping:Seq[Int]) = {
    keyMapping.zipWithIndex.toMap
  }
}
