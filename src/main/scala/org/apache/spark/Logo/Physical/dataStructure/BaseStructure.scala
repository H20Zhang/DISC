package org.apache.spark.Logo.Physical.dataStructure

import org.apache.spark.Logo.Physical.utlis.{ListGenerator, TestUtil}

class BaseStructure


//TODO finish this to optimize the code
class KeyMapping(val keyMapping:Map[Int,Int]) extends Serializable {
  def getKeys() = keyMapping.keys
  def getValues() = keyMapping.values

  def getSubKeyMapping(key:Seq[Int]) = {
    val keySet = key.toSet
    KeyMapping(keyMapping.filterKeys(p => keySet.contains(p)))
  }
  def toReverseMapping():KeyMapping = KeyMapping(keyMapping.map(_.swap))

  //transform the representation of the keyMapping to its list form.
  def toListMapping():Seq[Int] = {
    val sortedListMapTemp = keyMapping.toList.sortBy(_._1)
    require(sortedListMapTemp.zipWithIndex.forall(p => p._1._1 == p._2), "keymapping is not full rank, cannot convert to list representation")

    sortedListMapTemp.map(_._2)
  }

  override def equals(obj: scala.Any) = obj match {
    case t:KeyMapping =>
      t.keyMapping.toSeq.sortBy(_._1).zip(keyMapping.toSeq.sortBy(_._1)).forall(p => p._1 == p._2)
    case _ => false
  }
}

class PatternInstance(val pattern:Seq[Int]) extends Serializable {
  lazy val patternSize = pattern.size

  override def equals(obj: scala.Any): Boolean = obj match {
    case t:PatternInstance => TestUtil.listEqual(t.pattern,pattern)
    case _ => false
  }

  def toKeyPatternInstance():KeyPatternInstance = new KeyPatternInstance(pattern)
  def toValuePatternInstance():ValuePatternInstance = new ValuePatternInstance(pattern)
}

class KeyPatternInstance(pattern:Seq[Int]) extends PatternInstance(pattern)
class ValuePatternInstance(pattern:Seq[Int]) extends  PatternInstance(pattern)

object KeyMapping {
  def apply(keyMapping: List[Int]): KeyMapping = new KeyMapping(keyMapping.zipWithIndex.map(_.swap).toMap)
  def apply(keyMapping: Map[Int, Int]): KeyMapping = new KeyMapping(keyMapping)
  def apply(keyMapping: Int*):KeyMapping = new KeyMapping(keyMapping.zipWithIndex.map(_.swap).toMap)
}

object PatternInstance{

  def apply(pattern: Seq[Int]): PatternInstance = new PatternInstance(pattern)
  def build(lInstance:PatternInstance, lKeyMapping:KeyMapping, rInstance:PatternInstance, rKeyMapping:KeyMapping):PatternInstance = {

    val totalNodes = (lKeyMapping.getValues() ++ rKeyMapping.getValues()).toSeq.max + 1

    apply(ListGenerator.fillListIntoTargetList(
      rInstance.pattern,
      totalNodes,
      rKeyMapping.toListMapping(),
      ListGenerator.fillListIntoSlots(lInstance.pattern,totalNodes,lKeyMapping.toListMapping())))
  }
}




