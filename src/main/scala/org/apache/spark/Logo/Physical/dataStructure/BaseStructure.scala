package org.apache.spark.Logo.Physical.dataStructure

import org.apache.spark.Logo.Physical.utlis.{ListGenerator, ListSelector, TestUtil}

class BaseStructure

//TODO finish this to optimize the code
class KeyMapping(val keyMapping:Map[Int,Int]) extends Serializable {


  def contains(key:Int) = {
    keyMapping.contains(key)
  }
  def apply(key:Int): Int = keyMapping(key)

  def getNumOfKey() = keyMapping.size
  def getKeys() = keyMapping.keys.toSeq
  def getValues() = keyMapping.values.toSeq

  def keySet() = keyMapping.keySet

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

  def toList() = {
    keyMapping.toList
  }

  def toMap():Map[Int,Int] = {
    keyMapping
  }

  //determine if the keyMapping contain all key from 0 to n.
  def isFullRank():Boolean = {
    keyMapping.toList.sortBy(_._1).zipWithIndex.forall(p => p._1._1 == p._2)
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

  override def toString: String = {
    pattern.toString()
  }

  def subPatterns(colToPreserve:Seq[Int]) = {
    PatternInstance(ListSelector.selectElements(pattern,colToPreserve))
  }
}

class KeyPatternInstance(pattern:Seq[Int]) extends PatternInstance(pattern)
class ValuePatternInstance(pattern:Seq[Int]) extends  PatternInstance(pattern)

object KeyMapping {
  def apply(keyMapping: Seq[Int]): KeyMapping = new KeyMapping(keyMapping.zipWithIndex.map(_.swap).toMap)
  def apply(keyMapping: Map[Int, Int]): KeyMapping = new KeyMapping(keyMapping)


  implicit def MapToKeyMapping(theMap:Map[Int,Int]) = {
    apply(theMap)
  }
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

object KeyPatternInstance{
  def apply(pattern: Seq[Int]): KeyPatternInstance = new KeyPatternInstance(pattern)
}

object ValuePatternInstance{
  def apply(pattern: Seq[Int]): ValuePatternInstance = new ValuePatternInstance(pattern)
}


