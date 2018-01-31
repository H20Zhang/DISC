package org.apache.spark.Logo.UnderLying.dataStructure

import scala.collection.mutable.ArrayBuffer
import scala.collection.{AbstractIterator, Iterator, mutable}
import org.apache.spark.Logo.UnderLying.utlis.MapBuilder

import scala.reflect.ClassTag


trait LogoBlockRef extends Serializable

/**
  * Convinent method for ouputting the count result, for testing
  *
  * @param count
  */
class CountLogo(val count: Long) extends LogoBlockRef {}

class DebugLogo(val message: String, val value: Long = 0L) extends LogoBlockRef {}

class LogoBlock[A: ClassTag](val schema: LogoSchema, val metaData: LogoMetaData, val rawData: A) extends LogoBlockRef {

}


class RowLogoBlock[A: ClassTag](schema: LogoSchema, metaData: LogoMetaData, rawData: Seq[A]) extends LogoBlock(schema, metaData, rawData) {}

//TODO finish this
class CompositeLogoBlock(schema: LogoSchema, metaData: LogoMetaData, rawData: Seq[LogoBlockRef], handler: (Seq[LogoBlockRef], CompositeLogoSchema) => LogoBlockRef) extends LogoBlock(schema, metaData, rawData) {
  def executeHandler() = ???
}

class FileBasedLogoBlock;

class CompressedLogoBlock[A: ClassTag, B: ClassTag](schema: LogoSchema, metaData: LogoMetaData, rawEdge: Seq[B], crystals: Seq[(String, List[B])], rawAttr: A)


//TODO finish below

/**
  * Basic LogoBlock Class for UnlabeledPatternMatching
  *
  * @param schema   schema for the block
  * @param metaData metaData for the block
  * @param rawData  rawData for the block
  */
abstract class PatternLogoBlock[A: ClassTag](schema: LogoSchema, metaData: LogoMetaData, rawData: A) extends LogoBlock(schema, metaData, rawData) {

  //TODO testing required for below, this place needs further optimization
  def buildIndex(schema: KeyValueLogoSchema) = {
    val rawData = assemble()
    val keys = schema.keys.toSet


    if (keys.size == 1) {

      //      MapBuilder.fromListToMapLongFast(rawData.map(_.pattern),keys,keys.toSeq)

      MapBuilder.fromListToMapLongFastCompact(rawData.map(_.pattern), keys, keys.toSeq)

    } else if (keys.size == 2) {

      //      MapBuilder.fromListToMapLongFast(rawData.map(_.pattern),keys,keys.toSeq)

      MapBuilder.fromListToMapLongFastCompact(rawData.map(_.pattern), keys, keys.toSeq)

    } else {
      null.asInstanceOf[mutable.LongMap[CompactPatternList]]
    }


  }

  //sub class needs to over-write this method
  def iterator(): Iterator[PatternInstance]

  def enumerateIterator(): Iterator[PatternInstance]

  def assemble(): Seq[PatternInstance] = iterator().toList

  def toKeyValueLogoBlock(key: Set[Int]): KeyValuePatternLogoBlock = {

    val keyValueSchema = new KeyValueLogoSchema(schema, key)
    val keyValueRawData = buildIndex(keyValueSchema)
    new KeyValuePatternLogoBlock(
      keyValueSchema,
      metaData,
      keyValueRawData
    )
  }

  def toConcreteLogoBlock: ConcretePatternLogoBlock = {
    new ConcretePatternLogoBlock(
      schema,
      metaData,
      assemble()
    )
  }

  def toFilteringLogoBlock(f: PatternInstance => Boolean): FilteringPatternLogoBlock[A] = {
    new FilteringPatternLogoBlock(this, f)
  }

  override def toString: String = {
    s"schema:\n${schema.toString}\nmetaData:${metaData.toString}\nrawData:${rawData.toString()}"
  }
}

/**
  * Basic LogoBlock Class in which pattern is concretified.
  *
  * @param schema   schema for the block
  * @param metaData metaData for the block
  * @param rawData  rawData for the block, here we assume the nodeIds are represented using Int.
  */
class ConcretePatternLogoBlock(schema: LogoSchema, metaData: LogoMetaData, rawData: Seq[PatternInstance]) extends PatternLogoBlock(schema, metaData, rawData) {
  override def assemble(): Seq[PatternInstance] = rawData

  override def iterator(): Iterator[PatternInstance] = rawData.iterator

  override def enumerateIterator(): Iterator[PatternInstance] = iterator()

}

class CompactConcretePatternLogoBlock(schema: LogoSchema, metaData: LogoMetaData, rawData: CompactPatternList) extends PatternLogoBlock(schema, metaData, rawData) {
  override def assemble(): Seq[PatternInstance] = rawData.iterator.toList

  override def iterator(): Iterator[PatternInstance] = rawData.iterator

  override def enumerateIterator(): Iterator[PatternInstance] = iterator()
}

/**
  * Basic LogoBlock Class representing edges.
  *
  * @param schema   schema for the block
  * @param metaData metaData for the block
  * @param rawData  rawData for the block, here we assume the nodeIds are represented using Int.
  */
final class EdgePatternLogoBlock(schema: LogoSchema, metaData: LogoMetaData, rawData: Seq[PatternInstance]) extends ConcretePatternLogoBlock(schema, metaData, rawData) {

}


//TODO test required
/**
  * LogoBlock which can be used as a key-value map.
  *
  * @param schema   schema for the block
  * @param metaData metaData for the block
  * @param rawData  rawData for the block, here we assume the nodeIds are represented using Int.
  */
final class KeyValuePatternLogoBlock(schema: KeyValueLogoSchema, metaData: LogoMetaData, rawData: mutable.LongMap[CompactPatternList]) extends PatternLogoBlock(schema, metaData, rawData) {
  def valueMapping(keyMapping: KeyMapping) = KeyMapping(schema.valueKeyMapping(keyMapping))

  //get the values from the key in KeyValuePatternLogoBlock
  def getValue(key: KeyPatternInstance) = {

    val resRaw = rawData.getOrElse(key.node, null)
    val res = resRaw match {
      case null => None
      case _ => Some(resRaw)
    }

    res
  }


  //TODO this part is wrong, it is only just a temporary fix
  override def iterator() = null

  //    rawData.toSeq.flatMap(f => f._2).iterator

  override def enumerateIterator(): Iterator[PatternInstance] = null

  //    iterator()
}

trait enumerateIterator {
  def longSize():Long
}

//TODO: test filtering
final class FilteringPatternLogoBlock[A: ClassTag](var logoBlock: PatternLogoBlock[A], var f: PatternInstance => Boolean) extends PatternLogoBlock(logoBlock.schema, logoBlock.metaData, logoBlock.rawData) {


  var num:Long = 0L

  def toFilteringIterator(it: Iterator[PatternInstance]): Iterator[PatternInstance] = new AbstractIterator[PatternInstance] with enumerateIterator {

    private var hd: PatternInstance = _

    final def hasNext: Boolean = {
      do {
        if (!it.hasNext) return false
        hd = it.next()
      } while (!f(hd))
      true
    }

    final def next() = hd

    override def longSize():Long = {
      while (it.hasNext){
        if (f(it.next())){
          num = num + 1
        }
      }
      num
    }
  }

  override def iterator(): Iterator[PatternInstance] = toFilteringIterator(logoBlock.iterator())

  override def enumerateIterator(): Iterator[PatternInstance] = toFilteringIterator(logoBlock.enumerateIterator())

  //  override def write(kryo: Kryo, output: Output): Unit = {
  //    kryo.writeObject(output,logoBlock)
  //    kryo.writeObject(output,f)
  //  }
  //
  //  override def read(kryo: Kryo, input: Input): Unit = {
  //    logoBlock = kryo.readObject(input,classOf[PatternLogoBlock[A]])
  //    f = kryo.readObject(input,classOf[PatternInstance=>Boolean])
  //  }
}




/**
  * Composite LogoBlock Class for UnlabeledPatternMatching, which is assembled using multiple basic PatternLogoBlock.
  * In this specific class, there are only two blocks to build the new composite block.s
  *
  * @param schema   composite schema for the block
  * @param metaData metaData for the block
  * @param rawData  rawData is other PatternLogoBlocks which assembled this block
  */
final class CompositeTwoPatternLogoBlock(schema: PlannedTwoCompositeLogoSchema, metaData: LogoMetaData, rawData: Seq[PatternLogoBlock[_]]) extends PatternLogoBlock(schema, metaData, rawData) {

  lazy val coreBlock = schema.getCoreBlock(rawData)
  lazy val leafsBlock = schema.getLeafBlock(rawData)
  lazy val coreLeafJoints = schema.getCoreLeafJoins()
  lazy val coreKeyMapping = schema.coreKeyMapping
  lazy val leafValueMapping = schema.leafBlockSchema.valueKeyMapping(schema.leafKeyMapping)
  lazy val valueMapping = leafsBlock.valueMapping(schema.leafKeyMapping)

  //TODO testing required


  def arrayToIterator(array: Array[ValuePatternInstance]): Iterator[ValuePatternInstance] = {

    val res = new AbstractIterator[ValuePatternInstance] {

      private var cur = 0
      private var end = array.length
      private var curEle: ValuePatternInstance = _

      final def hasNext: Boolean = cur < end

      final def next(): ValuePatternInstance = {

        curEle = array(cur)
        cur += 1
        curEle
      }

    }
    res
  }

  val coreJointsSeq = coreLeafJoints.coreJoints.toSeq.sorted

  //generate the leaf instance grow from this core, actually the Iterator is just a wrapper the leafs are concrefied
  //the intersection node is not included in the returned iterator.
  def genereateLeafsNode(coreInstance: PatternInstance): Iterator[ValuePatternInstance] = {

    //    val optValues = leafsBlock.
    //      getValue(coreInstance.subPatterns(coreLeafJoints.coreJoints).toKeyPatternInstance())

    val optValues = leafsBlock.
      getValue(coreInstance.toSubKeyPattern(coreLeafJoints.coreJoints, coreJointsSeq))


    optValues match {
      case Some(values) => values.iterator //arrayToIterator(values)
      case _ => null
    }
  }

  def oneValueGenereateLeafsNode(coreInstance: PatternInstance): Array[Int] = {

    //    val optValues = leafsBlock.
    //      getValue(coreInstance.subPatterns(coreLeafJoints.coreJoints).toKeyPatternInstance())

    val optValues = leafsBlock.
      getValue(coreInstance.toSubKeyPattern(coreLeafJoints.coreJoints, coreJointsSeq))


    optValues match {
      case Some(values) => values.getRaw() //arrayToIterator(values)
      case _ => null
    }
  }


  @transient lazy val totalNodes = (coreKeyMapping.values ++ valueMapping.values).toSeq.max + 1

  //assmeble the core and leafs instance into a single instance
  def assembleCoreAndLeafInstance(coreInstance: PatternInstance, leafInstanceNode: ValuePatternInstance): PatternInstance = {

    //    PatternInstance.slowBuild(
    //      coreInstance,
    //      coreKeyMapping,
    //      leafInstanceNode,
    //      valueMapping,
    //      totalNodes
    //    )

    PatternInstance.quickBuild(
      coreInstance,
      coreKeyMapping,
      leafInstanceNode,
      valueMapping,
      totalNodes
    )
  }

  //the iterator to iterate through the pattern, core is iterated but leaf are concrefied but treat as a iterator for convinence.
  final class patternIterator extends Iterator[PatternInstance] {

    var leafsIterator: Iterator[ValuePatternInstance] = _
    val coreIterator: Iterator[PatternInstance] = coreBlock.iterator()
    var currentCore: PatternInstance = _

    override def hasNext: Boolean = {

      if (leafsIterator == null || !leafsIterator.hasNext) {
        do {
          if (coreIterator.hasNext) {
            currentCore = coreIterator.next()
          } else {
            return false
          }
          leafsIterator = genereateLeafsNode(currentCore)
        } while (leafsIterator == null)
      }

      return true
    }

    override def next(): PatternInstance = {
      val leafs = leafsIterator.next()
      val core = currentCore
      assembleCoreAndLeafInstance(core, leafs)
    }
  }

  final class EnumerateIterator extends Iterator[PatternInstance] with enumerateIterator {

    var leafsIterator: Iterator[ValuePatternInstance] = _
    val coreIterator: Iterator[PatternInstance] = coreBlock.enumerateIterator()
    var currentCore: PatternInstance = _
    //    val TintBuffer = new TIntArrayList(totalNodes)
    val array = Array.fill(totalNodes)(0)
    val currentPattern: EnumeratePatternInstance = new EnumeratePatternInstance(array)
    val coreMapping = coreKeyMapping.keyMapping.toArray

    val coreMapping1 = coreMapping.map(_._1)
    val coreMapping2 = coreMapping.map(_._2)

    val valueKeyMapping = valueMapping.keyMapping.toArray

    val valueKeyMapping1 = valueKeyMapping.map(_._1)
    val valueKeyMapping2 = valueKeyMapping.map(_._2)

    val valueKeyMapping1Value = valueKeyMapping1.size match {
      case 1 => valueKeyMapping1(0)
      case _ => 0
    }

    val valueKeyMapping2Value = valueKeyMapping2.size match {
      case 1 => valueKeyMapping2(0)
      case _ => 0
    }

    val updateArray = coreMapping ++ valueKeyMapping
    val coreLen = coreMapping.length
    val valueLen = valueKeyMapping.length


    val valueMappingSize = valueMapping.keyMapping.size

    def hasNext: Boolean = {

      if (leafsIterator == null || !leafsIterator.hasNext) {
        do {
          if (coreIterator.hasNext) {
            currentCore = coreIterator.next()

            var i = 0

            while (i < coreLen) {
              array.update(coreMapping2(i), currentCore.pattern(coreMapping1(i)))
              i += 1
            }


          } else {
            return false
          }
          leafsIterator = genereateLeafsNode(currentCore)

        } while (leafsIterator == null)
      }

      val leafs = leafsIterator.next()

      if (valueMappingSize != 0) {
        var i = 0

        while (i < valueLen) {
          array.update(valueKeyMapping2(i), leafs.getValue(valueKeyMapping1(i)))
          i += 1
        }
      }

      return true
    }


    override def next(): PatternInstance = {
      currentPattern
    }

    var longCount = 0
    override def longSize(): Long = {
      while (hasNext){
        longCount += 1
      }

      longCount
    }
  }


  final class OneValueLenEnumerateIterator extends Iterator[PatternInstance] with enumerateIterator {

    var curLeafPos = 0
    var leafEnd = 0
    var leafsIterator: Array[Int] = _
    val coreIterator: Iterator[PatternInstance] = coreBlock.enumerateIterator()
    var currentCore: PatternInstance = _
    //    val TintBuffer = new TIntArrayList(totalNodes)
    val array = Array.fill(totalNodes)(0)
    val currentPattern: EnumeratePatternInstance = new EnumeratePatternInstance(array)
    val coreMapping = coreKeyMapping.keyMapping.toArray

    val coreMapping1 = coreMapping.map(_._1)
    val coreMapping2 = coreMapping.map(_._2)

    val valueKeyMapping = valueMapping.keyMapping.toArray

    val valueKeyMapping1 = valueKeyMapping.map(_._1)
    val valueKeyMapping2 = valueKeyMapping.map(_._2)

    val valueKeyMapping1Value = valueKeyMapping1.size match {
      case 1 => valueKeyMapping1(0)
      case _ => 0
    }

    val valueKeyMapping2Value = valueKeyMapping2.size match {
      case 1 => valueKeyMapping2(0)
      case _ => 0
    }

    val updateArray = coreMapping ++ valueKeyMapping
    val coreLen = coreMapping.length
    val valueLen = valueKeyMapping.length

    val isCoreIteratorEnumerate = coreIterator.isInstanceOf[enumerateIterator]
    val valueMappingSize = valueMapping.keyMapping.size


    private def moveToNext: Option[Boolean] = {
      do {
        if (coreIterator.hasNext) {

          currentCore = coreIterator.next()

          var i = 0
          while (i < coreLen) {
            array.update(coreMapping2(i), currentCore.pattern(coreMapping1(i)))
            i += 1
          }

        } else {
          return Some(false)
        }
        curLeafPos = 0
        leafsIterator = oneValueGenereateLeafsNode(currentCore)

        if (leafsIterator != null){
          leafEnd = leafsIterator.length
        }


      } while (leafsIterator == null)
      None
    }

    def hasNext: Boolean = {

      if (leafsIterator == null || curLeafPos == leafEnd) {
        moveToNext match {
          case Some(toReturn) => return toReturn
          case None =>
        }
      }

      val leafs = leafsIterator(curLeafPos)
      array.update(valueKeyMapping2Value, leafs)
      curLeafPos += 1

      return true
    }

    override def next(): PatternInstance = {
      currentPattern
    }

    var longCount = 0
    override def longSize(): Long = {
      while (hasNext){
        longCount += 1
      }

      longCount
    }
  }


  //  class filterNullIterator extends Iterator[PatternInstance]{
  //
  //    private var it = new OneValueNullLenEnumerateIterator
  //    private var hd: PatternInstance = _
  //
  //    final def hasNext: Boolean =  {
  //      do {
  //        if (!it.hasNext) return false
  //        hd = it.next()
  //      } while (hd == null)
  //      true
  //    }
  //
  //    final def next() = hd
  //
  //  }
  //
  //  class OneValueNullLenEnumerateIterator extends Iterator[PatternInstance] {
  //
  //    var leafsIterator: Iterator[ValuePatternInstance] = _
  //    val coreIterator: Iterator[PatternInstance] = coreBlock.enumerateIterator()
  //    var currentCore: PatternInstance = _
  //    //    val TintBuffer = new TIntArrayList(totalNodes)
  //    val array = Array.fill(totalNodes)(0)
  //    val currentPattern: EnumeratePatternInstance = new EnumeratePatternInstance(array)
  //    val coreMapping = coreKeyMapping.keyMapping.toArray
  //
  //    val coreMapping1 = coreMapping.map(_._1)
  //    val coreMapping2 = coreMapping.map(_._2)
  //
  //    val valueKeyMapping = valueMapping.keyMapping.toArray
  //
  //    val valueKeyMapping1 = valueKeyMapping.map(_._1)
  //    val valueKeyMapping2 = valueKeyMapping.map(_._2)
  //
  //    val valueKeyMapping1Value = valueKeyMapping1.size match {
  //      case 1 => valueKeyMapping1(0)
  //      case _ => 0
  //    }
  //
  //    val valueKeyMapping2Value = valueKeyMapping2.size match {
  //      case 1 => valueKeyMapping2(0)
  //      case _ => 0
  //    }
  //
  //    val updateArray = coreMapping ++ valueKeyMapping
  //    val coreLen = coreMapping.length
  //    val valueLen = valueKeyMapping.length
  //
  //
  //    val valueMappingSize = valueMapping.keyMapping.size
  //
  //    def hasNext: Boolean = {
  //
  //      if (leafsIterator == null || !leafsIterator.hasNext) {
  //        do {
  //          if (coreIterator.hasNext) {
  //            currentCore = coreIterator.next()
  //
  //            var i = 0
  //
  //            while (i < coreLen) {
  //              array.update(coreMapping2(i), currentCore.pattern(coreMapping1(i)))
  //              i += 1
  //            }
  //
  //
  //          } else {
  //            return false
  //          }
  //          leafsIterator = genereateLeafsNode(currentCore)
  //
  //        } while (leafsIterator == null)
  //      }
  //
  //
  //      return true
  //    }
  //
  //    val nullPattern: PatternInstance = null
  //
  //    override def next(): PatternInstance = {
  //      val leafs = leafsIterator.next()
  //      array.update(valueKeyMapping2Value, leafs.getValue(valueKeyMapping2Value))
  //
  //      if (currentCore.contain(leafs.getValue(valueKeyMapping2Value))) {
  //        nullPattern
  //      } else {
  //        currentPattern
  //      }
  //    }
  //  }

  /**
    * generate a ConcretePatternLogoBlock
    */
  override def iterator() = new patternIterator

  override def enumerateIterator() = valueMapping.keyMapping.toArray.size match {
    case 1 => new OneValueLenEnumerateIterator
    case _ => new EnumerateIterator
  }

}


//TODO now we limit the implementation to that leaf and leaf must have intesection, only work for intList intersection intList, more generic version should be implemented.
/**
  * Composite LogoBlock Class for UnlabeledPatternMatching, which is assembled using multiple basic PatternLogoBlock.
  * In this specific class, there are only two blocks to build the new composite block.s
  *
  * @param schema   composite schema for the block
  * @param metaData metaData for the block
  * @param rawData  rawData is other PatternLogoBlocks which assembled this block
  */
final class CompositeThreePatternLogoBlock(schema: PlannedThreeCompositeLogoSchema, metaData: LogoMetaData, rawData: Seq[PatternLogoBlock[_]]) extends PatternLogoBlock(schema, metaData, rawData) {

  lazy val coreBlock = schema.getCoreBlock(rawData)
  lazy val leftLeafsBlock = schema.getLeftLeafBlock(rawData)
  lazy val rightLeafsBlock = schema.getRightLeafBlock(rawData)
  lazy val leftCoreLeafJoints = schema.getCoreLeftLeafJoins()
  lazy val rightCoreLeafJoints = schema.getCoreRightLeafJoins()
  lazy val leafLeafJoints = schema.getLeafLeafJoints()

  lazy val coreKeyMapping = schema.coreKeyMapping
  lazy val leftLeafValueMapping = schema.leftLeafBlockSchema.valueKeyMapping(schema.leftLeafKeyMapping)
  lazy val rightLeafValueMapping = schema.rightLeafBlockSchema.valueKeyMapping(schema.rightLeafKeyMapping)
  lazy val leftvalueMapping = leftLeafsBlock.valueMapping(schema.leftLeafKeyMapping)
  lazy val rightvalueMapping = leftLeafsBlock.valueMapping(schema.leftLeafKeyMapping)

  //TODO testing required

  val leftCoreJointsSeq = leftCoreLeafJoints.coreJoints.toSeq.sorted
  val rightCoreJointsSeq = rightCoreLeafJoints.coreJoints.toSeq.sorted

  //generate the leaf instance grow from this core, actually the Iterator is just a wrapper the leafs are concrefied
  //the intersection node is not included in the returned iterator.
  def genereateLeftLeafsNode(coreInstance: PatternInstance): Array[Int] = {

    val optValues = leftLeafsBlock.
      getValue(coreInstance.toSubKeyPattern(leftCoreLeafJoints.coreJoints, leftCoreJointsSeq))

    optValues match {
      case Some(values) => values.getRaw()
      case _ => null
    }
  }

  //generate the leaf instance grow from this core, actually the Iterator is just a wrapper the leafs are concrefied
  //the intersection node is not included in the returned iterator.
  def genereateRightLeafsNode(coreInstance: PatternInstance): Array[Int] = {

    val optValues = rightLeafsBlock.
      getValue(coreInstance.toSubKeyPattern(rightCoreLeafJoints.coreJoints, rightCoreJointsSeq))

    optValues match {
      case Some(values) => values.getRaw()
      case _ => null
    }
  }


  @transient lazy val totalNodes = (coreKeyMapping.values ++ leftvalueMapping.values ++ rightvalueMapping.values).toSeq.max + 1

  //assmeble the core and leafs instance into a single instance
  def assembleCoreAndLeafInstance(coreInstance: PatternInstance, leftLeafInstanceNode: ValuePatternInstance): PatternInstance = {

    //    PatternInstance.slowBuild(
    //      coreInstance,
    //      coreKeyMapping,
    //      leafInstanceNode,
    //      valueMapping,
    //      totalNodes
    //    )

    PatternInstance.quickBuild(
      coreInstance,
      coreKeyMapping,
      leftLeafInstanceNode,
      leftvalueMapping,
      totalNodes
    )
  }


  //TODO this method currently only work for one node cases
  def generateIntersectionIterator(leftIterator: Array[Int], rightIterator: Array[Int]): Iterator[Int] = {

    if (leftIterator == null || !(leftIterator.size != 0)) {
      return new AbstractIterator[Int] {
        override def hasNext: Boolean = false

        override def next(): Int = 0
      }
    }

    if (rightIterator == null || !(rightIterator.size != 0)) {
      return new AbstractIterator[Int] {
        override def hasNext: Boolean = false

        override def next(): Int = 0
      }
    }

    new AbstractIterator[Int] {

      var nextEle: Int = 0
      var leftArray = leftIterator
      var rightArray = rightIterator
      var uCur = 0
      var vCur = 0
      val uD = leftArray.size
      val vD = rightArray.size
      val buffer = ArrayBuffer[Int]()
      var bufferFilled = false
      fillBuffer()
      val bufferedIterator = buffer.iterator

      //      final override def hasNext: Boolean = {
      //
      //        while ( {
      //          (uCur < uD) && (vCur < vD)
      //        }) {
      //
      //          if (leftArray(uCur) < rightArray(vCur)) uCur += 1
      //          else if (leftArray(uCur) > rightArray(vCur)) vCur += 1
      //          else {
      //            nextEle = leftArray(uCur)
      //            uCur += 1
      //            vCur += 1
      //            return true
      //          }
      //        }
      //
      //        return false
      //      }

      final override def hasNext: Boolean = {
        bufferedIterator.hasNext
      }

      def fillBuffer(): Unit = {
        while ( {
          (uCur < uD) && (vCur < vD)
        }) {

          if (leftArray(uCur) < rightArray(vCur)) uCur += 1
          else if (leftArray(uCur) > rightArray(vCur)) vCur += 1
          else {
            buffer += leftArray(uCur)
            uCur += 1
            vCur += 1
          }
        }
      }


      final override def next(): Int = {
        bufferedIterator.next()
      }
    }

  }


  //  {
  //    @throws[InterruptedException]
  //    private def intersect(uN: IndexedSeq[Int], vN: IndexedSeq[Int], node:Int): Long = {
  //      if ((uN == null) || (vN == null)) return 0L
  //      var count = 0L
  //      var uCur = 0
  //      var vCur = 0
  //      val uD = uN.size
  //      val vD = vN.size
  //      while ( {
  //        (uCur < uD) && (vCur < vD)
  //      }) if (uN(uCur) < vN(vCur)) uCur += 1
  //      else if (vN(vCur) < uN(uCur)) vCur += 1
  //      else {
  //        if (uN(uCur) > node) count += 1L
  //        uCur += 1
  //        vCur += 1
  //      }
  //      count
  //    }
  //  }

  //the iterator to iterate through the pattern, core is iterated but leaf are concrefied but treat as a iterator for convinence.
  final class patternIterator extends Iterator[PatternInstance] {

    var leftLeafsIterator: Array[Int] = _
    var rightLeafsIterator: Array[Int] = _
    var intersectIterator: Iterator[Int] = _
    val coreIterator: Iterator[PatternInstance] = coreBlock.iterator()
    var currentCore: PatternInstance = _

    override def hasNext: Boolean = {

      if (intersectIterator == null || !intersectIterator.hasNext) {
        do {
          if (coreIterator.hasNext) {
            currentCore = coreIterator.next()
          } else {
            return false
          }
          leftLeafsIterator = genereateLeftLeafsNode(currentCore)
          rightLeafsIterator = genereateRightLeafsNode(currentCore)
          intersectIterator = generateIntersectionIterator(leftLeafsIterator, rightLeafsIterator)

        } while (!intersectIterator.hasNext)
      }

      return true
    }

    override def next(): PatternInstance = {
      val leafs = ValuePatternInstance(intersectIterator.next())
      val core = currentCore
      assembleCoreAndLeafInstance(core, leafs)
    }
  }

  final class EnumerateIterator extends Iterator[PatternInstance] with enumerateIterator {

    var leftLeafsIterator: Array[Int] = _
    var rightLeafsIterator: Array[Int] = _
    var intersectIterator: Iterator[Int] = _
    val coreIterator: Iterator[PatternInstance] = coreBlock.enumerateIterator()
    var currentCore: PatternInstance = _
    //    val TintBuffer = new TIntArrayList(totalNodes)
    val array = Array.fill(totalNodes)(0)
    val currentPattern: EnumeratePatternInstance = new EnumeratePatternInstance(array)
    val coreMapping = coreKeyMapping.keyMapping.toArray

    val coreMapping1 = coreMapping.map(_._1)
    val coreMapping2 = coreMapping.map(_._2)

    val valueKeyMapping = leftvalueMapping.keyMapping.toArray

    val valueKeyMapping1 = valueKeyMapping.map(_._1)
    val valueKeyMapping2 = valueKeyMapping.map(_._2)

    val updateArray = coreMapping ++ valueKeyMapping
    val coreLen = coreMapping.length
    val valueLen = valueKeyMapping.length

    val valueKeyMapping2Value = valueKeyMapping2(0)
    val valueKeyMapping1Value = valueKeyMapping1(0)


    private def moveToNext: Option[Boolean] = {
      do {
        if (coreIterator.hasNext) {
          currentCore = coreIterator.next()
          var i = 0
          while (i < coreLen) {
            array.update(coreMapping2(i), currentCore.pattern(coreMapping1(i)))
            i += 1
          }
        } else {
          return Some(false)
        }
        leftLeafsIterator = genereateLeftLeafsNode(currentCore)
        rightLeafsIterator = genereateRightLeafsNode(currentCore)
        intersectIterator = generateIntersectionIterator(leftLeafsIterator, rightLeafsIterator)

      } while (!intersectIterator.hasNext)
      None
    }

    override def hasNext: Boolean = {

      if (intersectIterator == null || !intersectIterator.hasNext) {
        moveToNext match {
          case Some(toReturn) => return toReturn
          case None =>
        }
      }

      val leafs = intersectIterator.next()
      array.update(valueKeyMapping2Value, leafs)

      return true
    }

    override def next(): PatternInstance = {
      currentPattern
    }

    var longCount = 0
    override def longSize(): Long = {
      while (hasNext){
        longCount += 1
      }

      longCount
    }

  }

  /**
    * generate a ConcretePatternLogoBlock
    */
  override def iterator() = new patternIterator

  override def enumerateIterator() = new EnumerateIterator

}

//TODO finish the LogoBlock for block-centric iterative process
class IterativeLogoBlock

class IterativeNodeLogoBlock extends IterativeLogoBlock

class IterativeEdgeLogoBlock extends IterativeLogoBlock






