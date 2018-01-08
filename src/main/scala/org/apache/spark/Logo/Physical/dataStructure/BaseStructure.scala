package org.apache.spark.Logo.Physical.dataStructure

import org.apache.spark.Logo.Physical.utlis.{ListGenerator, ListSelector, TestUtil}

import scala.collection.mutable.ArrayBuffer

class BaseStructure

//TODO finish this to optimize the code
class KeyMapping(val keyMapping:Map[Int,Int]) extends Serializable {


  @transient lazy val listMapping = toListMapping()

  def contains(key:Int) = {
    keyMapping.contains(key)
  }
  def apply(key:Int): Int = keyMapping(key)

  def getNumOfKey() = keyMapping.size
  def getKeys() = keyMapping.keys.toSeq
  def getValues() = values
  @transient lazy val values = keyMapping.values.toSeq


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



  override def toString: String = {
    toList().toString()
  }
}


//TODO optimize hash for node and edge PatternInstance
class PatternInstance(var pattern:Seq[Int]) extends Serializable {
  lazy val patternSize = pattern.size

  def toOneKeyPatternInstance():KeyPatternInstance = new OneKeyPatternInstance(pattern(0))
  def toTwoKeyPatternInstance():KeyPatternInstance = new TwoKeyPatternInstance(pattern(0),pattern(1))
  def toKeyPatternInstance():KeyPatternInstance = new KeyPatternInstance(pattern)
  def toValuePatternInstance():ValuePatternInstance = new ValuePatternInstance(pattern)

  override def toString: String = {
    pattern.toString()
  }

  def subPatterns(colToPreserve:Set[Int]) = {
    PatternInstance(ListSelector.selectElements(pattern,colToPreserve))
  }

  def toSubKeyPattern(colToPreserveSet:Set[Int], colToPreserve:Seq[Int]) = {
    val colSizes = colToPreserveSet.size
    val res = colSizes match {
      case 1 => new OneKeyPatternInstance(pattern(colToPreserve(0)))
      case 2 => new TwoKeyPatternInstance(pattern(colToPreserve(0)),pattern(colToPreserve(1)))
      case _ => subPatterns(colToPreserveSet).toKeyPatternInstance()
    }

    res
  }


  def canEqual(other: Any): Boolean = other.isInstanceOf[PatternInstance]

  override def equals(other: Any): Boolean = other match {
    case that: PatternInstance =>
      (that canEqual this) &&
        patternSize == that.patternSize &&
        pattern == that.pattern
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(pattern)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

class EnumeratePatternInstance(pattern:ArrayBuffer[Int]) extends PatternInstance(pattern){
}

class KeyPatternInstance(pattern:Seq[Int]) extends PatternInstance(pattern){

}


class OneKeyPatternInstance(pattern0:Int) extends KeyPatternInstance(null){
  val node = pattern0

  override def canEqual(other: Any): Boolean = other.isInstanceOf[OneKeyPatternInstance]

  override def equals(other: Any): Boolean = other match {
    case that: OneKeyPatternInstance =>
        node == that.node
    case _ => false
  }

  override def hashCode(): Int = {
    31*node
  }
}


class TwoKeyPatternInstance(pattern0:Int, pattern1:Int) extends KeyPatternInstance(null){

  val node0 = pattern0
  val node1 = pattern1

  override def canEqual(other: Any): Boolean = other.isInstanceOf[OneKeyPatternInstance]


  override def equals(other: Any): Boolean = other match {
    case that: TwoKeyPatternInstance =>
        node0 == that.node0 &&
        node1 == that.node1
    case _ => false
  }

  override def hashCode(): Int = {
    31*node0+node1
  }
}

class ValuePatternInstance(pattern:Seq[Int]) extends  PatternInstance(pattern){

}

object KeyMapping extends Serializable {
  def apply(keyMapping: Seq[Int]): KeyMapping = new KeyMapping(keyMapping.zipWithIndex.map(_.swap).toMap)
  def apply(keyMapping: Map[Int, Int]): KeyMapping = new KeyMapping(keyMapping)


  implicit def MapToKeyMapping(theMap:Map[Int,Int]) = {
    apply(theMap)
  }
}

object PatternInstance extends Serializable {

  def apply(pattern: Seq[Int]): PatternInstance = new PatternInstance(pattern)
  def slowBuild(lInstance:PatternInstance, lKeyMapping:KeyMapping, rInstance:PatternInstance, rKeyMapping:KeyMapping, totalNodes:Int):PatternInstance = {

    if (rKeyMapping.keyMapping.size == 0){
      lInstance
    } else {
      apply(ListGenerator.fillListIntoTargetList(
        rInstance.pattern,
        totalNodes,
        rKeyMapping.listMapping,
        ListGenerator.fillListIntoSlots(lInstance.pattern,totalNodes,lKeyMapping.listMapping)))
    }
  }

  def quickBuild(lInstance:PatternInstance, lKeyMapping:KeyMapping, rInstance:PatternInstance, rKeyMapping:KeyMapping,totalNodes:Int) = {

    if (rKeyMapping.keyMapping.size == 0){
      lInstance
    }else {
      val arrayBuffer = new Array[Int](totalNodes)
      lKeyMapping.keyMapping.foreach{f =>
        arrayBuffer(f._2) = lInstance.pattern(f._1)}

      rKeyMapping.keyMapping.foreach{f =>
        arrayBuffer(f._2) = rInstance.pattern(f._1)}

      apply(arrayBuffer)
    }
  }
}

object KeyPatternInstance extends Serializable {
  def apply(pattern: Seq[Int]): KeyPatternInstance = new KeyPatternInstance(pattern)
}




object ValuePatternInstance extends Serializable {
  def apply(pattern: Seq[Int]): ValuePatternInstance = new ValuePatternInstance(pattern)
}


