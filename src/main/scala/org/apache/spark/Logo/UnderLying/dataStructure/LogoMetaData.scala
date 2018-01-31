package org.apache.spark.Logo.UnderLying.dataStructure


/**
  * recording the meta data for each logoBlock
  *
  * @param color         the keys of the logoBlock
  * @param numberOfParts how many concrete Logo Part this LogoBlock has stored
  */
case class LogoMetaData(color: Seq[Int], numberOfParts: Long) extends Serializable


//TODO this part although currently is not used, it will be used later.
class CompositeMetaData(color: Seq[Int], subColors: Seq[Seq[Int]], subNumberOfParts: Seq[Long]) extends LogoMetaData(color, subNumberOfParts(0))
