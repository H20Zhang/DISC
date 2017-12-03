package org.apache.spark.Logo.Physical.dataStructure


/**
  * recording the meta data for each logoBlock
  * @param color the keys of the logoBlock
  * @param schema the schema for this logoBlock
  * @param numberOfParts how many concrete Logo Part this LogoBlock has stored
  */
case class LogoMetaData (color:Seq[Int], schema: LogoSchema, numberOfParts:Long) extends Serializable

