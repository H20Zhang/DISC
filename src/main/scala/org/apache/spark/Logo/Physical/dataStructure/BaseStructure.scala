package org.apache.spark.Logo.Physical.dataStructure

import org.apache.spark.Logo.Physical.utlis.{ListGenerator, ListSelector, TestUtil}

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
class PatternInstance(val pattern:Seq[Int]) extends Serializable {
  lazy val patternSize = pattern.size



  def toOneKeyPatternInstance():KeyPatternInstance = new OneKeyPatternInstance(pattern(0))
  def toKeyPatternInstance():KeyPatternInstance = new KeyPatternInstance(pattern)
  def toValuePatternInstance():ValuePatternInstance = new ValuePatternInstance(pattern)

  override def toString: String = {
    pattern.toString()
  }

  def subPatterns(colToPreserve:Set[Int]) = {
    PatternInstance(ListSelector.selectElements(pattern,colToPreserve))
  }

  def toSubKeyPattern(colToPreserve:Set[Int]) = {
    val colSizes = colToPreserve.size
    val res = colSizes match {
      case 1 => new OneKeyPatternInstance(pattern(colToPreserve.toSeq(0)))
      case _ => subPatterns(colToPreserve).toKeyPatternInstance()
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

class KeyPatternInstance(pattern:Seq[Int]) extends PatternInstance(pattern){

}


class OneKeyPatternInstance(pattern:Int) extends KeyPatternInstance(Seq(pattern)){
  val node = pattern

  override def canEqual(other: Any): Boolean = other.isInstanceOf[OneKeyPatternInstance]

  override def equals(other: Any): Boolean = other match {
    case that: OneKeyPatternInstance =>
      super.equals(that) &&
        (that canEqual this) &&
        node == that.node
    case _ => false
  }

  override def hashCode(): Int = {
    31*node
  }
}

//TODO finish this class
class TwoKeyPatternInstance(pattern:Seq[Int]) extends KeyPatternInstance(pattern){

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
  def build(lInstance:PatternInstance, lKeyMapping:KeyMapping, rInstance:PatternInstance, rKeyMapping:KeyMapping,totalNodes:Int):PatternInstance = {


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
  
}

object KeyPatternInstance extends Serializable {
  def apply(pattern: Seq[Int]): KeyPatternInstance = new KeyPatternInstance(pattern)
}




object ValuePatternInstance extends Serializable {
  def apply(pattern: Seq[Int]): ValuePatternInstance = new ValuePatternInstance(pattern)
}


