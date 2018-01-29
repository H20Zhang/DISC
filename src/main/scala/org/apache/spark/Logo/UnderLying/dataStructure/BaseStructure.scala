package org.apache.spark.Logo.UnderLying.dataStructure

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import gnu.trove.list.array.TIntArrayList
import org.apache.spark.Logo.UnderLying.utlis.{ListGenerator, ListSelector, TestUtil}

import scala.collection.mutable.ArrayBuffer


class BaseStructure




//TODO finish this to optimize the code

/**
  * The class that represent a mapping, and provide a series of convenient methods
  * @param keyMapping
  */
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
/**
  * The class that represent a pattern instance, which can later be converted to keyPatternInstance and valuePatternInstance
  * @param pattern Seq[Int] used to represent pattern
  */
class PatternInstance(var pattern:Array[Int]) extends Serializable with KryoSerializable{
//  lazy val patternSize = pattern.size

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

  //for keyPattern with just one node or two node, we make specific optimization here.
  def toSubKeyPattern(colToPreserveSet:Set[Int], colToPreserve:Seq[Int]) = {

    val colSizes = colToPreserve.size
    val res = colSizes match {
      case 1 => new OneKeyPatternInstance(pattern(colToPreserve(0)))
      case 2 => new TwoKeyPatternInstance(pattern(colToPreserve(0)),pattern(colToPreserve(1)))
      case _ => throw new Exception("keys must be 2 or 1, more need to be implemented")
    }

    res
  }


  def canEqual(other: Any): Boolean = other.isInstanceOf[PatternInstance]

  override def equals(other: Any): Boolean = other match {
    case that: PatternInstance =>
      (that canEqual this) &&
        pattern == that.pattern
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(pattern)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    output.writeInt(pattern.size,true)
    output.writeInts(pattern.toArray,true)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    val size = input.readInt(true)
    pattern = input.readInts(size,true)
  }
}

final class EnumeratePatternInstance(pattern:Array[Int]) extends PatternInstance(pattern){
}

class KeyPatternInstance(pattern:Seq[Int]) extends PatternInstance(null){
  var node = 0L
}

/**
  * keyPattern for with just one node, for performance improvement
  * @param pattern0
  */
final class OneKeyPatternInstance(pattern0:Int) extends KeyPatternInstance(null){
  node = pattern0.toLong

  override def canEqual(other: Any): Boolean = other.isInstanceOf[OneKeyPatternInstance]

  override def equals(other: Any): Boolean = other match {
    case that: OneKeyPatternInstance =>
        node == that.node
    case _ => false
  }

  override def hashCode(): Int = {
    (node*31).hashCode()
//    MurmurHash3.finalizeHash(MurmurHash3.mix(0x3c074a61,node),1)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    node = input.readLong(true)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    output.writeLong(node,true)
  }
}

/**
  * keyPattern with just two node
  * @param pattern0
  * @param pattern1
  */
final class TwoKeyPatternInstance(pattern0:Int, pattern1:Int) extends KeyPatternInstance(null){

//  var node0 = pattern0
//  var node1 = pattern1
//
//
  node = (pattern0.toLong << 32) | (pattern1 & 0xffffffffL)
//
//  val x: Int = (l >> 32).toInt
//  val y: Int = l.toInt
//

  override def canEqual(other: Any): Boolean = other.isInstanceOf[OneKeyPatternInstance]


  override def equals(other: Any): Boolean = other match {
    case that: TwoKeyPatternInstance =>
        node == that.node
    case _ => false
  }

  override def hashCode(): Int = {
//    Hash.fmix32(node0)
    (node*31).hashCode()
//    MurmurHash3.finalizeHash(MurmurHash3.mix(MurmurHash3.mix(0x3c074a61,node0),node1),2)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    node = input.readLong(true)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    output.writeLong(node,true)
  }
}


class rawPatternInstance(tIntArrayList: TIntArrayList) extends PatternInstance(null){
  override def toSubKeyPattern(colToPreserveSet: Set[Int], colToPreserve: Seq[Int]): KeyPatternInstance = {


      val colSizes = colToPreserveSet.size
      val res = colSizes match {
        case 1 => new OneKeyPatternInstance(tIntArrayList.getQuick((colToPreserve(0))))
        case 2 => new TwoKeyPatternInstance(tIntArrayList.getQuick(colToPreserve(0)),tIntArrayList.getQuick(colToPreserve(1)))
      }

      res


  }
}


class ValuePatternInstance(pattern1:Array[Int]) extends  PatternInstance(pattern1){

  def getValue(idx:Int): Int ={
    pattern(idx)
  }
}


final class OneValuePatternInstance(var node1:Int) extends ValuePatternInstance(null){
  override def getValue(idx: Int): Int = {
    node1
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    output.writeInt(node1,true)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    node1 = input.readInt(true)
  }


}

final class TwoValuePatternInstance(var node1:Int, var node2:Int) extends ValuePatternInstance(null){
  override def getValue(idx: Int): Int = {
    if (idx == 1){
      node1
    } else {
      node2
    }
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    output.writeInt(node1,true)
    output.writeInt(node2,true)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    node1 = input.readInt(true)
    node2 = input.readInt(true)
  }


}

object KeyMapping extends Serializable {
  def apply(keyMapping: Seq[Int]): KeyMapping = new KeyMapping(keyMapping.zipWithIndex.map(_.swap).toMap)
  def apply(keyMapping: Map[Int, Int]): KeyMapping = new KeyMapping(keyMapping)


  implicit def MapToKeyMapping(theMap:Map[Int,Int]) = {
    apply(theMap)
  }
}

object PatternInstance extends Serializable{

  def apply(pattern: Seq[Int]): PatternInstance = new PatternInstance(pattern.toArray)
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

  def quickBuild(lInstance:PatternInstance, lKeyMapping:KeyMapping, rInstance:ValuePatternInstance, rKeyMapping:KeyMapping, totalNodes:Int) = {

    if (rKeyMapping.keyMapping.size == 0){
      lInstance
    }else {

      val array = new Array[Int](totalNodes)
      lKeyMapping.keyMapping.foreach{f =>
        array(f._2) = lInstance.pattern(f._1)}

      rKeyMapping.keyMapping.foreach{f =>
        array(f._2) = rInstance.getValue(f._1)}

      apply(array)
    }
  }

  def quickBuild(lInstance:PatternInstance, lKeyMapping:KeyMapping, rInstance:PatternInstance, rKeyMapping:KeyMapping, rrInstance:PatternInstance, rrKeyMapping:KeyMapping, totalNodes:Int) = {

    if (rKeyMapping.keyMapping.size == 0 && rrKeyMapping.keyMapping.size == 0){
      lInstance
    }else {

      val array = new Array[Int](totalNodes)
      lKeyMapping.keyMapping.foreach{f =>
        array(f._2) = lInstance.pattern(f._1)}

      rKeyMapping.keyMapping.foreach{f =>
        array(f._2) = rInstance.pattern(f._1)}

      rrKeyMapping.keyMapping.foreach{f =>
        array(f._2) = rrInstance.pattern(f._1)}

      apply(array)
    }
  }


}

object KeyPatternInstance extends Serializable {
  def apply(pattern: Seq[Int]): KeyPatternInstance = new KeyPatternInstance(pattern)
}


object ValuePatternInstance extends Serializable {
  def apply(pattern: Seq[Int]): ValuePatternInstance = {
    if (pattern.size == 1){
      apply(pattern(0))
    } else if (pattern.size == 2){
      apply(pattern(0),pattern(1))
    } else{
      new ValuePatternInstance(pattern.toArray)
    }
  }
  def apply(node1:Int): ValuePatternInstance = new OneValuePatternInstance(node1)
  def apply(node1:Int, node2:Int): ValuePatternInstance = new TwoValuePatternInstance(node1, node2)
}



//TODO: test
trait compactPatternList{
  def iterator:Iterator[PatternInstance]
}


class CompactArrayPatternList(var rawData:Array[Int], patternWitdth:Int) extends compactPatternList {

  class enumeratePatternIterator extends Iterator[EnumeratePatternInstance]{

    var cur = 0
    val end = rawData.length
    val array = Array.fill(patternWitdth)(0)
    val currentPattern:EnumeratePatternInstance = new EnumeratePatternInstance(array)


    override def hasNext: Boolean = cur  < end

    override def next(): EnumeratePatternInstance = {
      var i = 0
      while (i < patternWitdth){
        array(i) = rawData(cur + i)
        i += 1
      }
      cur += patternWitdth
      currentPattern
    }
  }

  def iterator:Iterator[EnumeratePatternInstance] = new enumeratePatternIterator
}

class CompactOnePatternList(var rawData:Array[Int]) extends compactPatternList {

  class PatternIterator extends Iterator[PatternInstance]{

    var cur = 0
    val end = rawData.length
    val currentPattern:OneValuePatternInstance = new OneValuePatternInstance(0)


    override def hasNext: Boolean = cur < end

    override def next(): PatternInstance = {
      currentPattern.node1 = rawData(cur)
      cur += 1
      currentPattern
    }
  }

  def iterator:Iterator[PatternInstance] = new PatternIterator
}

class CompactTwoPatternList(var rawData:Array[Int]) extends compactPatternList {

  class PatternIterator extends Iterator[PatternInstance]{

    var cur = 0
    val end = rawData.length
    val currentPattern:TwoValuePatternInstance = new TwoValuePatternInstance(0,0)


    override def hasNext: Boolean = cur < end

    override def next(): PatternInstance = {
      currentPattern.node1 = rawData(cur)
      currentPattern.node2 = rawData(cur+1)
      cur += 2
      currentPattern
    }
  }

  def iterator:Iterator[PatternInstance] = new PatternIterator
}

class CompactListAppendBuilder(patternWidth:Int) {
  var arrayBuffer = new ArrayBuffer[Int]()

  def append(node:Int): Unit ={
    arrayBuffer.append(node)
  }

  def toCompactList() = {
    if (patternWidth == 1){
      new CompactOnePatternList(arrayBuffer.toArray)
    } else if(patternWidth == 2){
      new CompactTwoPatternList(arrayBuffer.toArray)
    } else{
      new CompactArrayPatternList(arrayBuffer.toArray,patternWidth)
    }
  }
}


object CompactListBuilder{

  def apply(data:Seq[Seq[Int]], patternWidth:Int):compactPatternList  = {
    if (patternWidth == 1){
      new CompactOnePatternList(data.map(f => f(0)).toArray)
    } else if (patternWidth == 2){

      val array = new Array[Int](data.size * 2)
      var i = 0
      data.foreach{f =>
        array(i) = f(0)
        array(i+1) = f(1)
        i += 2
      }
      new CompactTwoPatternList(array)
    } else{
      val array = new Array[Int](data.size * patternWidth)
      var i = 0
      data.foreach{f =>
        var j = 0
        while (j < patternWidth){
          array(i +j) = f(j)
          j += 1
        }
        i += patternWidth
      }
      new CompactArrayPatternList(array,patternWidth)
    }
  }
}



