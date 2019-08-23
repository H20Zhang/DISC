package org.apache.spark.adj.deprecated.execution.rdd

import java.util

import org.apache.spark.adj.leapfrog.Alg
import org.apache.spark.adj.deprecated.plan.deprecated.PhysicalPlan.FilteringCondition
import org.apache.spark.adj.utlis.MapBuilder

import scala.collection.mutable.ArrayBuffer
import scala.collection.{AbstractIterator, Iterator, mutable}
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
  def buildIndex(schema: KeyValueLogoSchema,needSorting:Boolean) = {
//    val rawData = assemble()
    val keys = schema.keys.toSet

    if (keys.size == 1) {

      //      MapBuilder.fromListToMapLongFast(rawData.map(_.pattern),keys,keys.toSeq)

      MapBuilder.fromListToMapLongFastCompact(enumerateIterator(), keys, keys.toSeq, schema.keyCol.toSeq.diff(schema.keys.toSeq),schema.keyCol.size-keys.size, needSorting)

    } else if (keys.size == 2) {

      //      MapBuilder.fromListToMapLongFast(rawData.map(_.pattern),keys,keys.toSeq)

      MapBuilder.fromListToMapLongFastCompact(enumerateIterator(), keys, keys.toSeq, schema.keyCol.toSeq.diff(schema.keys.toSeq),schema.keyCol.size-keys.size, needSorting)

    } else {
      null.asInstanceOf[mutable.LongMap[CompactPatternList]]
    }

  }

  //sub class needs to over-write this method
  def iterator(): Iterator[PatternInstance]

  def enumerateIterator(): Iterator[PatternInstance]

  def assemble(): Seq[PatternInstance] = {
    val array = iterator().toBuffer
    array
  }

  def toKeyValueLogoBlock(key: Set[Int],needSorting:Boolean): KeyValuePatternLogoBlock = {

    val keyValueSchema = new KeyValueLogoSchema(schema, key)
    val keyValueRawData = buildIndex(keyValueSchema,needSorting)
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

  def toFilteringLogoBlock(f: FilteringCondition): FilteringPatternLogoBlock[A] = {
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
  override def assemble(): Seq[PatternInstance] = {
    val array = rawData.iterator.toArray
    array
  }

  override def iterator(): Iterator[PatternInstance] = rawData.iterator()
  override def enumerateIterator(): Iterator[PatternInstance] = rawData.iterator()
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
    resRaw
  }


  //TODO this part is wrong, it is only just a temporary fix
  override def iterator() = null

  //    rawData.toSeq.flatMap(f => f._2).iterator

  override def enumerateIterator(): Iterator[PatternInstance] = null

  //    iterator()
}

trait enumerateIterator {
  def approximateLongSize():Long
  def longSize():Long
  def setFilteringCondition(filteringCondition:FilteringCondition):Unit
}

trait generateIterator {
  def setArray(array:Array[Int]):Unit
}

//TODO: test filtering
final class FilteringPatternLogoBlock[A: ClassTag](var logoBlock: PatternLogoBlock[A], var fc: FilteringCondition) extends PatternLogoBlock(logoBlock.schema, logoBlock.metaData, logoBlock.rawData) {


  lazy val f = fc.f

  lazy val nodeVerifyTuple = fc.nodesNotSameTuple


  def toFilteringIterator(it: Iterator[PatternInstance]): Iterator[PatternInstance] = new AbstractIterator[PatternInstance] with enumerateIterator {

    var num:Long = 0L
    private var hd: PatternInstance = _

//    final def hasNext: Boolean = {
//      do {
//        if (!it.hasNext) return false
//        hd = it.next()
//      } while (!f(hd))
//      true
//    }

    private var hdDefined: Boolean = false

    final def hasNext: Boolean = hdDefined || {
      do {
        if (!it.hasNext) return false
        hd = it.next()
      } while (!f(hd))
      hdDefined = true
      true
    }

    final def next() = { hdDefined = false; hd }



//    final def next() = hd

    override def longSize():Long = {

      if (it.isInstanceOf[enumerateIterator]){
        val enumerateIt = it.asInstanceOf[enumerateIterator]
        enumerateIt.setFilteringCondition(fc)
        num = enumerateIt.longSize()
      } else {
        while (true){
          if (!it.hasNext){
            return num
          }

          if (f(it.next())){
            num = num + 1
          }

        }
      }

      num
    }

    override def setFilteringCondition(filteringCondition: FilteringCondition): Unit = {
      require(filteringCondition == null, "this iterator shouldn't accpet another filtering condition")
    }

    override def approximateLongSize(): Long = {

      if (it.isInstanceOf[enumerateIterator]){
        val enumerateIt = it.asInstanceOf[enumerateIterator]
        enumerateIt.setFilteringCondition(fc)
        num = enumerateIt.approximateLongSize()
      } else {
        while (true){
          if (!it.hasNext){
            return num
          }

          if (f(it.next())){
            num = num + 1
          }

        }
      }

      num
    }
  }


  def toFilteringIteratorForNode(it: Iterator[PatternInstance]): Iterator[PatternInstance] = new AbstractIterator[PatternInstance] with enumerateIterator {

    var num:Long = 0L
    private var hd: PatternInstance = _

    final def hasNext: Boolean = {
      do {
        if (!it.hasNext) return false
        hd = it.next()

      } while (nodeVerifyTuple.forall(f => hd.getValue(f._1) == hd.getValue(f._2)))
      true
    }

    final def next() = hd

    override def longSize():Long = {
      while (true){
        if (!it.hasNext){
          return num
        }

        hd = it.next()
        if (!nodeVerifyTuple.forall(f => hd.getValue(f._1) == hd.getValue(f._2))){
          num = num + 1
        }
      }
      num
    }

    override def setFilteringCondition(filteringCondition: FilteringCondition): Unit = {
      require(filteringCondition == null, "this iterator shouldn't accpet another filtering condition")
    }

    override def approximateLongSize(): Long = {
      while (true){
        if (!it.hasNext){
          return num
        }

        hd = it.next()
        if (!nodeVerifyTuple.forall(f => hd.getValue(f._1) == hd.getValue(f._2))){
          num = num + 1
        }
      }
      num
    }
  }

  def toFilteringIteratorForNode2(it: Iterator[PatternInstance]): Iterator[PatternInstance] = new AbstractIterator[PatternInstance] with enumerateIterator {

    var num:Long = 0L
    val v10 = nodeVerifyTuple(0)._1
    val v20 = nodeVerifyTuple(0)._2
    val v11 = nodeVerifyTuple(1)._1
    val v21 = nodeVerifyTuple(1)._2

    private var hd: PatternInstance = _

    final def hasNext: Boolean = {
      do {
        if (!it.hasNext) return false
        hd = it.next()

      } while (hd.getValue(v10) == hd.getValue(v20) || hd.getValue(v11) == hd.getValue(v21))
      true
    }

    final def next() = hd

    override def longSize():Long = {

      while (true){
        if (!it.hasNext){
          return num
        }

        hd = it.next()

        val p = hd.pattern

        if (p(v10) != p(v20) && p(v10) != p(v21)){
          num = num + 1
        }
      }
      num
    }

    override def setFilteringCondition(filteringCondition: FilteringCondition): Unit = {
      require(filteringCondition == null, "this iterator shouldn't accpet another filtering condition")
    }

    override def approximateLongSize(): Long = {
      while (true){
        if (!it.hasNext){
          return num
        }

        hd = it.next()

        val p = hd.pattern

        if (p(v10) != p(v20) && p(v10) != p(v21)){
          num = num + 1
        }
      }
      num
    }
  }

  override def iterator(): Iterator[PatternInstance] = toFilteringIterator(logoBlock.iterator())


  //TODO test and select the best performance
  override def enumerateIterator(): Iterator[PatternInstance] = {

    if (fc.f != null){
      toFilteringIterator(logoBlock.enumerateIterator())
    } else {
      if (fc.nodesNotSameTuple.length == 2){
        toFilteringIteratorForNode2(logoBlock.enumerateIterator())
      } else{
        toFilteringIteratorForNode(logoBlock.enumerateIterator())
      }
    }








  }
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

    if (optValues != null){
      optValues.iterator()
    } else{
      null
    }
  }

  def oneValueGenereateLeafsNode(coreInstance: PatternInstance): Array[Int] = {

    val optValues = leafsBlock.
      getValue(coreInstance.toSubKeyPattern(coreLeafJoints.coreJoints, coreJointsSeq))

    if (optValues != null){
      optValues.getRaw()
    } else {
      null
    }
  }

  def zeroValueGenereateLeafsNode(coreInstance: PatternInstance): Array[Int] = {

    val optValues = leafsBlock.
      getValue(coreInstance.toSubKeyPattern(coreLeafJoints.coreJoints, coreJointsSeq))

    if (optValues != null){
      Array(0)
    } else {
      null
    }
  }

  val coreKeyMappingArray = coreKeyMapping.keyMapping.toArray
  val valueMappingArray = valueMapping.keyMapping.toArray
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
      coreKeyMappingArray,
      leafInstanceNode,
      valueMappingArray,
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
//    val array = new FixSizeArray(totalNodes)
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
              array.update(coreMapping2(i), currentCore.getValue(coreMapping1(i)))
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

    var filteringCondition:FilteringCondition = null

    override def setFilteringCondition(filteringCondition: FilteringCondition): Unit = {
      this.filteringCondition = filteringCondition
    }

    var longCount = 0L
    override def longSize(): Long = {

      if (filteringCondition == null){
        while (hasNext){
          longCount += 1
        }
      } else {
        val f = filteringCondition.f
        while (hasNext){
          if (f(currentPattern)){
            longCount += 1
          }
        }
      }

      longCount
    }

    override def approximateLongSize(): Long = {
      if (filteringCondition == null){
        while (hasNext){
          longCount += 1
        }
      } else {
        val f = filteringCondition.f
        while (hasNext){
          if (f(currentPattern)){
            longCount += 1
          }
        }
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

//    val array = new FixSizeArray(totalNodes)

    val currentPattern: EnumeratePatternInstance = new EnumeratePatternInstance(array)
    val coreMapping = coreKeyMapping.keyMapping.toArray

    val coreMapping1 = coreMapping.map(_._1)
    val coreMapping2 = coreMapping.map(_._2)

    val valueKeyMapping = valueMapping.keyMapping.toArray

    val valueKeyMapping1 = valueKeyMapping.map(_._1)
    val valueKeyMapping2 = valueKeyMapping.map(_._2)

    final val valueKeyMapping1Value = valueKeyMapping1.size match {
      case 1 => valueKeyMapping1(0)
      case _ => 0
    }

    final val valueKeyMapping2Value = valueKeyMapping2.size match {
      case 1 => valueKeyMapping2(0)
      case _ => 0
    }

    val updateArray = coreMapping ++ valueKeyMapping
    final val coreLen = coreMapping.length
    final val valueLen = valueKeyMapping.length

    val isCoreIteratorEnumerate = coreIterator.isInstanceOf[enumerateIterator]
    val valueMappingSize = valueMapping.keyMapping.size

    private def moveToNext: Option[Boolean] = {
      do {

        if (coreIterator.hasNext) {

          currentCore = coreIterator.next()

          var i = 0
          while (i < coreLen) {
            array.update(coreMapping2(i), currentCore.getValue(coreMapping1(i)))
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

//      val leafs = leafsIterator(curLeafPos)
//      array.update(valueKeyMapping2Value, leafs)
//      curLeafPos += 1

      return true
    }

    override def next(): PatternInstance = {

      val leafs = leafsIterator(curLeafPos)
      array.update(valueKeyMapping2Value, leafs)
      curLeafPos += 1

      currentPattern
    }

    var filteringCondition:FilteringCondition = null

    override def setFilteringCondition(filteringCondition: FilteringCondition): Unit = {
      this.filteringCondition = filteringCondition
    }

    var longCount = 0L
    override def longSize(): Long = {

      if (filteringCondition == null){
        while (true){

          moveToNext match {
            case Some(toReturn) => return longCount
            case None => {
              while (curLeafPos != leafEnd){

                val leafs = leafsIterator(curLeafPos)
                array.update(valueKeyMapping2Value, leafs)
                curLeafPos += 1
                longCount += 1
              }
            }
          }
        }
      } else{
        val f = filteringCondition.f
        while (true){
          moveToNext match {
            case Some(toReturn) => return longCount
            case None => {
              while (curLeafPos != leafEnd){

                val leafs = leafsIterator(curLeafPos)
                array.update(valueKeyMapping2Value, leafs)
                curLeafPos += 1

                if (f(currentPattern)){
                  longCount += 1
                }

              }
            }
          }
        }
      }


      longCount


    }

    override def approximateLongSize(): Long = {
      if (filteringCondition == null){
        while (true){

          moveToNext match {
            case Some(toReturn) => return longCount
            case None => {
              while (curLeafPos != leafEnd){
                curLeafPos += leafEnd
                longCount += array.size
              }
            }
          }
        }
      } else{
        val f = filteringCondition.f
        while (true){
          moveToNext match {
            case Some(toReturn) => return longCount
            case None => {
              while (curLeafPos != leafEnd){

                val leafs = leafsIterator(curLeafPos)
                array.update(valueKeyMapping2Value, leafs)
                curLeafPos += 1

                if (f(currentPattern)){
                  longCount += 1
                }

              }
            }
          }
        }
      }


      longCount
    }
  }


  final class ZeroValueLenEnumerateIterator extends Iterator[PatternInstance] with enumerateIterator {

    var curLeafPos = 0
    var leafEnd = 0
    var leafsIterator: Array[Int] = _
    val coreIterator: Iterator[PatternInstance] = coreBlock.enumerateIterator()
    var currentCore: PatternInstance = _
    //    val TintBuffer = new TIntArrayList(totalNodes)
    val array = Array.fill(totalNodes)(0)

    //    val array = new FixSizeArray(totalNodes)

    val currentPattern: EnumeratePatternInstance = new EnumeratePatternInstance(array)
    val coreMapping = coreKeyMapping.keyMapping.toArray

    val coreMapping1 = coreMapping.map(_._1)
    val coreMapping2 = coreMapping.map(_._2)

    val updateArray = coreMapping
    final val coreLen = coreMapping.length
    final val valueLen = 0

    val isCoreIteratorEnumerate = coreIterator.isInstanceOf[enumerateIterator]
    val valueMappingSize = valueMapping.keyMapping.size

    private def moveToNext: Option[Boolean] = {
      do {
        if (coreIterator.hasNext) {

          currentCore = coreIterator.next()

          var i = 0
          while (i < coreLen) {
            array.update(coreMapping2(i), currentCore.getValue(coreMapping1(i)))
            i += 1
          }


        } else {
          return Some(false)
        }
        curLeafPos = 0
        leafsIterator = zeroValueGenereateLeafsNode(currentCore)

        if (leafsIterator != null){
          leafEnd = leafsIterator.length
        }


      } while (leafsIterator == null)
      None
    }

    def hasNext: Boolean = {

      if (leafsIterator == null) {
        moveToNext match {
          case Some(toReturn) => return toReturn
          case None =>
        }
      }


      return true
    }

    override def next(): PatternInstance = {
      curLeafPos += 1

      currentPattern
    }

    var filteringCondition:FilteringCondition = null

    override def setFilteringCondition(filteringCondition: FilteringCondition): Unit = {
      this.filteringCondition = filteringCondition
    }

    var longCount = 0L
    override def longSize(): Long = {

      if (filteringCondition == null){
        while (true){

          moveToNext match {
            case Some(toReturn) => return longCount
            case None => {

                longCount += 1
              }

          }
        }
      } else{
        val f = filteringCondition.f
        while (true){
          moveToNext match {
            case Some(toReturn) => return longCount
            case None => {

                if (f(currentPattern)){
                  longCount += 1
                }


            }
          }
        }
      }


      longCount


    }

    override def approximateLongSize(): Long = {
      if (filteringCondition == null){
        while (true){

          moveToNext match {
            case Some(toReturn) => return longCount
            case None => {

              longCount += 1
            }

          }
        }
      } else{
        val f = filteringCondition.f
        while (true){
          moveToNext match {
            case Some(toReturn) => return longCount
            case None => {

              if (f(currentPattern)){
                longCount += 1
              }


            }
          }
        }
      }


      longCount

    }
  }

  /**
    * generate a ConcretePatternLogoBlock
    */
  override def iterator() = new patternIterator

  override def enumerateIterator() = valueMapping.keyMapping.toArray.size match {
    case 0 => new ZeroValueLenEnumerateIterator
    case 1 => new OneValueLenEnumerateIterator
    case _ => new EnumerateIterator
  }

}


/**
  * GJ Join-2, Composite LogoBlock Class for UnlabeledPatternMatching, which is assembled using multiple basic PatternLogoBlock.
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

    if (optValues != null){
      optValues.getRaw()
    } else {
      null
    }

  }

  //generate the leaf instance grow from this core, actually the Iterator is just a wrapper the leafs are concrefied
  //the intersection node is not included in the returned iterator.
  def genereateRightLeafsNode(coreInstance: PatternInstance): Array[Int] = {

    val optValues = rightLeafsBlock.
      getValue(coreInstance.toSubKeyPattern(rightCoreLeafJoints.coreJoints, rightCoreJointsSeq))

    if (optValues != null){
      optValues.getRaw()
    } else {
      null
    }
  }


  @transient lazy val totalNodes = (coreKeyMapping.values ++ leftvalueMapping.values ++ rightvalueMapping.values).toSeq.max + 1


  val coreKeyMappingArray = coreKeyMapping.keyMapping.toArray
  val leftValueMappingArray = leftvalueMapping.keyMapping.toArray
  //assmeble the core and leafs instance into a single instance
  def assembleCoreAndLeafInstance(coreInstance: PatternInstance, leftLeafInstanceNode: ValuePatternInstance): PatternInstance = {
    PatternInstance.quickBuild(
      coreInstance,
      coreKeyMappingArray,
      leftLeafInstanceNode,
      leftValueMappingArray,
      totalNodes
    )
  }

//  val resultMap = new mutable.HashMap[(Array[Int],Array[Int]),ArrayBuffer[Int]]()


  abstract class resetableIterator extends Iterator[Int]{
    def reset():Unit
    def getArray():ArrayBuffer[Int]
  }


  var lastLeft:Array[Int] = null
  var lastRight:Array[Int] = null
  var lastResult:resetableIterator = null

  //TODO this method currently only work for one node cases
  def generateIntersectionIterator(leftIterator: Array[Int], rightIterator: Array[Int], isBuffer:Boolean): Array[Int] = {
   Alg.leapfrogIntersection(Array(leftIterator, rightIterator))
  }

  //the iterator to iterate through the pattern, core is iterated but leaf are concrefied but treat as a iterator for convinence.
  final class patternIterator extends Iterator[PatternInstance] {

    var leftLeafsIterator: Array[Int] = _
    var rightLeafsIterator: Array[Int] = _
    var intersectIterator: Array[Int] = _
    val coreIterator: Iterator[PatternInstance] = coreBlock.iterator()
    var currentCore: PatternInstance = _

    var cur = 0
    var end = 0

    override def hasNext: Boolean = {

      if (intersectIterator == null || cur == end) {
        do {
          if (coreIterator.hasNext) {
            currentCore = coreIterator.next()
          } else {
            return false
          }
          leftLeafsIterator = genereateLeftLeafsNode(currentCore)
          rightLeafsIterator = genereateRightLeafsNode(currentCore)
          intersectIterator = generateIntersectionIterator(leftLeafsIterator, rightLeafsIterator, schema.gSync)
          cur = 0
          if (intersectIterator != null){
            end = intersectIterator.size
          }

        } while (intersectIterator == null || end == 0)
      }

      return true
    }

    val valuePatternInstance:OneValuePatternInstance = new OneValuePatternInstance(0)

    override def next(): PatternInstance = {
      valuePatternInstance.node1 = intersectIterator(cur)
      cur += 1
      assembleCoreAndLeafInstance(currentCore, valuePatternInstance)
    }
  }

  final class EnumerateIterator extends Iterator[PatternInstance] with enumerateIterator {

    var leftLeafsIterator: Array[Int] = _
    var rightLeafsIterator: Array[Int] = _
    var intersectIterator: Array[Int] = _
    val coreIterator: Iterator[PatternInstance] = coreBlock.enumerateIterator()
    var currentCore: PatternInstance = _
    //    val TintBuffer = new TIntArrayList(totalNodes)
    val array = Array.fill(totalNodes)(0)
    //    val array = new FixSizeArray(totalNodes)
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

    final val valueKeyMapping2Value = valueKeyMapping2(0)
    final val valueKeyMapping1Value = valueKeyMapping1(0)


    var cur = 0
    var end = 0

    //    private def generateAllCoreAndGroup = {
    //      coreBlock.iterator().toBuffer
    //    }

    private def moveToNext: Option[Boolean] = {
      do {
        if (coreIterator.hasNext) {
          currentCore = coreIterator.next()
          var i = 0
          while (i < coreLen) {
            array.update(coreMapping2(i), currentCore.getValue(coreMapping1(i)))
            i += 1
          }
        } else {
          return Some(false)
        }
        leftLeafsIterator = genereateLeftLeafsNode(currentCore)
        rightLeafsIterator = genereateRightLeafsNode(currentCore)
        intersectIterator = generateIntersectionIterator(leftLeafsIterator, rightLeafsIterator, schema.gSync)

        if (intersectIterator != null) {
          cur = 0
          end = intersectIterator.size
        }

      } while (intersectIterator == null || end == 0)
      None
    }

    override def hasNext: Boolean = {

      if (intersectIterator == null || cur == end) {
        moveToNext match {
          case Some(toReturn) => return toReturn
          case None =>
        }
      }

      val leafs = intersectIterator(cur)
      cur += 1
      array.update(valueKeyMapping2Value, leafs)

      return true
    }

    override def next(): PatternInstance = {
      currentPattern
    }

    var filteringCondition: FilteringCondition = null

    override def setFilteringCondition(filteringCondition: FilteringCondition): Unit = {
      this.filteringCondition = filteringCondition
    }

    var longCount = 0L

    override def longSize(): Long = {



      if (filteringCondition == null) {
        while (true) {

          moveToNext match {
            case Some(toReturn) => return longCount
            case None => {
              while (cur != end) {

                val leafs = intersectIterator(cur)
                array.update(valueKeyMapping2Value, leafs)
                cur += 1
                longCount += 1
              }
            }
          }
        }
      } else {
        val f = filteringCondition.f
        while (true) {
          moveToNext match {
            case Some(toReturn) => return longCount
            case None => {
              while (cur != end) {

                val leafs = intersectIterator(cur)
                array.update(valueKeyMapping2Value, leafs)
                cur += 1

                if (f(currentPattern)) {
                  longCount += 1
                }

              }
            }
          }
        }
      }

      longCount
    }

    override def approximateLongSize(): Long = {
      if (filteringCondition == null) {
        while (true) {

          moveToNext match {
            case Some(toReturn) => return longCount
            case None => {
              while (cur != end) {
                cur = end
                longCount += array.size
              }
            }
          }
        }
      } else {
        val f = filteringCondition.f
        while (true) {
          moveToNext match {
            case Some(toReturn) => return longCount
            case None => {
              while (cur != end) {

                val leafs = intersectIterator(cur)
                array.update(valueKeyMapping2Value, leafs)
                cur += 1

                if (f(currentPattern)) {
                  longCount += 1
                }

              }
            }
          }
        }
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


/**
  * GJ Join-3, Composite LogoBlock Class for UnlabeledPatternMatching, which is assembled using multiple basic PatternLogoBlock.
  * In this specific class, there are only two blocks to build the new composite block.s
  *
  * @param schema   composite schema for the block
  * @param metaData metaData for the block
  * @param rawData  rawData is other PatternLogoBlocks which assembled this block
  */
final class CompositeFourPatternLogoBlock(schema: PlannedFourCompositeLogoSchema, metaData: LogoMetaData, rawData: Seq[PatternLogoBlock[_]]) extends PatternLogoBlock(schema, metaData, rawData) {

  lazy val coreBlock = schema.getCoreBlock(rawData)
  lazy val leftLeafsBlock = schema.getLeftLeafBlock(rawData)
  lazy val rightLeafsBlock = schema.getRightLeafBlock(rawData)
  lazy val midLeafsBlock = schema.getMidLeafBlock(rawData)

  lazy val leftCoreLeafJoints = schema.getCoreLeftLeafJoins()
  lazy val rightCoreLeafJoints = schema.getCoreRightLeafJoins()
  lazy val midCoreLeafJoints = schema.getCoreMidLeafJoins()

  lazy val leafLeafJoints = schema.getLeafLeafJoints()

  lazy val coreKeyMapping = schema.coreKeyMapping

  lazy val leftLeafValueMapping = schema.leftLeafBlockSchema.valueKeyMapping(schema.leftLeafKeyMapping)
  lazy val rightLeafValueMapping = schema.rightLeafBlockSchema.valueKeyMapping(schema.rightLeafKeyMapping)
  lazy val midLeafValueMapping = schema.midLeafBlockSchema.valueKeyMapping(schema.midLeafKeyMapping)



  //TODO this place is strange, need to take a look.
  lazy val leftvalueMapping = leftLeafsBlock.valueMapping(schema.leftLeafKeyMapping)
  lazy val rightvalueMapping = leftLeafsBlock.valueMapping(schema.leftLeafKeyMapping)
  lazy val midvalueMapping = midLeafsBlock.valueMapping(schema.midLeafKeyMapping)


  val leftCoreJointsSeq = leftCoreLeafJoints.coreJoints.toSeq.sorted
  val rightCoreJointsSeq = rightCoreLeafJoints.coreJoints.toSeq.sorted
  val midCoreJointsSeq = midCoreLeafJoints.coreJoints.toSeq.sorted

  //generate the leaf instance grow from this core, actually the Iterator is just a wrapper the leafs are concrefied
  //the intersection node is not included in the returned iterator.
  def genereateLeftLeafsNode(coreInstance: PatternInstance): Array[Int] = {

    val optValues = leftLeafsBlock.
      getValue(coreInstance.toSubKeyPattern(leftCoreLeafJoints.coreJoints, leftCoreJointsSeq))

    if (optValues != null){
      optValues.getRaw()
    } else {
      null
    }

  }

  //generate the leaf instance grow from this core, actually the Iterator is just a wrapper the leafs are concrefied
  //the intersection node is not included in the returned iterator.
  def genereateRightLeafsNode(coreInstance: PatternInstance): Array[Int] = {

    val optValues = rightLeafsBlock.
      getValue(coreInstance.toSubKeyPattern(rightCoreLeafJoints.coreJoints, rightCoreJointsSeq))

    if (optValues != null){
      optValues.getRaw()
    } else {
      null
    }
  }

  //generate the leaf instance grow from this core, actually the Iterator is just a wrapper the leafs are concrefied
  //the intersection node is not included in the returned iterator.
  def genereateMidLeafsNode(coreInstance: PatternInstance): Array[Int] = {

    val optValues = midLeafsBlock.
      getValue(coreInstance.toSubKeyPattern(midCoreLeafJoints.coreJoints, midCoreJointsSeq))

    if (optValues != null){
      optValues.getRaw()
    } else {
      null
    }
  }


  @transient lazy val totalNodes = (coreKeyMapping.values ++ leftvalueMapping.values ++ rightvalueMapping.values ++ midvalueMapping.values).toSeq.max + 1


  val coreKeyMappingArray = coreKeyMapping.keyMapping.toArray
  val leftValueMappingArray = leftvalueMapping.keyMapping.toArray
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
      coreKeyMappingArray,
      leftLeafInstanceNode,
      leftValueMappingArray,
      totalNodes
    )
  }


  abstract class resetableIterator extends Iterator[Int]{
    def reset():Unit
    def getArray():ArrayBuffer[Int]
  }


  class reIterator(leftIterator:Array[Int], rightIterator:Array[Int]) extends resetableIterator{
    var nextEle: Int = 0

    var leftArray = leftIterator
    var rightArray = rightIterator

    var uCur = 0
    var vCur = 0
    val uD = leftArray.size
    val vD = rightArray.size
    val buffer = new ArrayBuffer[Int](Math.min(uD,vD))
    var bufferFilled = false
    val bound1 = 10
    val bound2 = 10
    fillBuffer()

    var bufferedIterator = buffer.iterator

    final override def hasNext: Boolean = {
      bufferedIterator.hasNext
    }


    //for balance cases
    def _fillBuffer1(): Unit = {
      while ( {
        (uCur < uD) && (vCur < vD)
      }) {

        val lValue = leftArray(uCur)
        val rValue = rightArray(vCur)

        if (lValue < rValue) uCur += 1
        else if (lValue > rValue) vCur += 1
        else {
          buffer += lValue
          uCur += 1
          vCur += 1
        }
      }
    }

    //for uD > vD * bound
    def _fillBuffer2(): Unit = {

      var prev = 0
      while (vCur < vD) {

        val temp = util.Arrays.binarySearch(leftArray, prev, uD, rightArray(vCur))
        if (temp >= 0) {
          buffer += rightArray(vCur)
          prev = temp
        }
        vCur += 1
      }
    }

    //for vD > uD * bound
    def _fillBuffer3(): Unit = {
      var prev = 0
      while (uCur < uD) {

        val temp = util.Arrays.binarySearch(rightArray, prev, vD, leftArray(uCur))
        if (temp >= 0) {
          buffer += leftArray(uCur)
          prev = temp
        }
        uCur += 1
      }
    }



    def fillBuffer(): Unit = {


      if (1+uD.toDouble/vD > bound1 * (Math.log(vD.toDouble)/Math.log(2))) {
        _fillBuffer2()
      } else if (1+vD.toDouble/uD > bound1 * (Math.log(uD.toDouble)/Math.log(2))) {
        _fillBuffer3()
      } else{
        _fillBuffer1()
      }
    }


    final override def next(): Int = {
      bufferedIterator.next()
    }

    override def reset(): Unit = {
      bufferedIterator = buffer.iterator
    }

    override def getArray(): ArrayBuffer[Int] = {
      buffer
    }
  }





  //TODO this method currently only work for one node cases
  def generateIntersectionIterator(leftIterator: Array[Int], rightIterator: Array[Int], midIterator: Array[Int]): Array[Int] = {
    Alg.leapfrogIntersection(Array(leftIterator, rightIterator, midIterator))
  }

  //the iterator to iterate through the pattern, core is iterated but leaf are concrefied but treat as a iterator for convinence.
  final class patternIterator extends Iterator[PatternInstance] {

    var leftLeafsIterator: Array[Int] = _
    var rightLeafsIterator: Array[Int] = _
    var midLeafsIterator: Array[Int] = _
    var intersectIterator: Array[Int] = _
    val coreIterator: Iterator[PatternInstance] = coreBlock.iterator()
    var currentCore: PatternInstance = _

    var cur = 0
    var end = 0

    override def hasNext: Boolean = {

      if (intersectIterator == null || cur == end) {
        do {
          if (coreIterator.hasNext) {
            currentCore = coreIterator.next()
          } else {
            return false
          }
          leftLeafsIterator = genereateLeftLeafsNode(currentCore)
          rightLeafsIterator = genereateRightLeafsNode(currentCore)
          midLeafsIterator = genereateMidLeafsNode(currentCore)
          intersectIterator = generateIntersectionIterator(leftLeafsIterator, rightLeafsIterator, midLeafsIterator)
          cur = 0
          if (intersectIterator != null){
            end = intersectIterator.size
          }

        } while (intersectIterator == null || end == 0)
      }

      return true
    }

    val valuePatternInstance:OneValuePatternInstance = new OneValuePatternInstance(0)

    override def next(): PatternInstance = {
      valuePatternInstance.node1 = intersectIterator(cur)
      cur += 1
      assembleCoreAndLeafInstance(currentCore, valuePatternInstance)
    }
  }

  final class EnumerateIterator extends Iterator[PatternInstance] with enumerateIterator {

    var leftLeafsIterator: Array[Int] = _
    var rightLeafsIterator: Array[Int] = _
    var midLeafsIterator: Array[Int] = _
    var intersectIterator: Array[Int] = _
    val coreIterator: Iterator[PatternInstance] = coreBlock.enumerateIterator()
    var currentCore: PatternInstance = _

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

    final val valueKeyMapping2Value = valueKeyMapping2(0)
    final val valueKeyMapping1Value = valueKeyMapping1(0)


    var cur = 0
    var end = 0


    private def moveToNext: Option[Boolean] = {
      do {
        if (coreIterator.hasNext) {
          currentCore = coreIterator.next()
          var i = 0
          while (i < coreLen) {
            array.update(coreMapping2(i), currentCore.getValue(coreMapping1(i)))
            i += 1
          }
        } else {
          return Some(false)
        }
        leftLeafsIterator = genereateLeftLeafsNode(currentCore)
        rightLeafsIterator = genereateRightLeafsNode(currentCore)
        midLeafsIterator = genereateMidLeafsNode(currentCore)
        intersectIterator = generateIntersectionIterator(leftLeafsIterator, rightLeafsIterator, midLeafsIterator)

        if (intersectIterator != null) {
          cur = 0
          end = intersectIterator.size
        }

      } while (intersectIterator == null || end == 0)
      None
    }

    override def hasNext: Boolean = {

      if (intersectIterator == null || cur == end) {
        moveToNext match {
          case Some(toReturn) => return toReturn
          case None =>
        }
      }

      val leafs = intersectIterator(cur)
      cur += 1
      array.update(valueKeyMapping2Value, leafs)

      return true
    }

    override def next(): PatternInstance = {
      currentPattern
    }

    var filteringCondition: FilteringCondition = null

    override def setFilteringCondition(filteringCondition: FilteringCondition): Unit = {
      this.filteringCondition = filteringCondition
    }

    var longCount = 0L

    override def longSize(): Long = {



      if (filteringCondition == null) {
        while (true) {

          moveToNext match {
            case Some(toReturn) => return longCount
            case None => {
              while (cur != end) {

                val leafs = intersectIterator(cur)
                array.update(valueKeyMapping2Value, leafs)
                cur += 1
                longCount += 1
              }
            }
          }
        }
      } else {
        val f = filteringCondition.f
        while (true) {
          moveToNext match {
            case Some(toReturn) => return longCount
            case None => {
              while (cur != end) {

                val leafs = intersectIterator(cur)
                array.update(valueKeyMapping2Value, leafs)
                cur += 1

                if (f(currentPattern)) {
                  longCount += 1
                }

              }
            }
          }
        }
      }

      longCount
    }

    override def approximateLongSize(): Long = {
      if (filteringCondition == null) {
        while (true) {

          moveToNext match {
            case Some(toReturn) => return longCount
            case None => {
              while (cur != end) {
                cur += end
                longCount += array.size
              }
            }
          }
        }
      } else {
        val f = filteringCondition.f
        while (true) {
          moveToNext match {
            case Some(toReturn) => return longCount
            case None => {
              while (cur != end) {

                val leafs = intersectIterator(cur)
                array.update(valueKeyMapping2Value, leafs)
                cur += 1

                if (f(currentPattern)) {
                  longCount += 1
                }

              }
            }
          }
        }
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






