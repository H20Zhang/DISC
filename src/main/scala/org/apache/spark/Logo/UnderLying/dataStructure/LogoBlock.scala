package org.apache.spark.Logo.UnderLying.dataStructure
import java.io.{Externalizable, ObjectInput, ObjectOutput}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import gnu.trove.list.array.TIntArrayList

import scala.collection.Iterator.empty
import scala.collection.{AbstractIterator, Iterator, mutable}
//import com.koloboke.collect.map.hash.HashObjObjMap
import org.apache.spark.Logo.UnderLying.utlis.{ListGenerator, ListSelector, MapBuilder}
import org.apache.spark.graphx.VertexId

import scala.Predef

import scala.reflect.ClassTag



trait LogoBlockRef extends Serializable

/**
  * Convinent method for ouputting the count result, for testing
  * @param count
  */
class CountLogo(val count:Long) extends LogoBlockRef{}
class DebugLogo(val message:String, val value:Long = 0L) extends LogoBlockRef{}

class LogoBlock[A:ClassTag](val schema: LogoSchema, val metaData: LogoMetaData, val rawData:A) extends LogoBlockRef{

}


class RowLogoBlock[A:ClassTag](schema: LogoSchema, metaData: LogoMetaData, rawData:Seq[A]) extends LogoBlock(schema, metaData, rawData){}

//TODO finish this
class CompositeLogoBlock(schema: LogoSchema, metaData: LogoMetaData, rawData:Seq[LogoBlockRef], handler:(Seq[LogoBlockRef],CompositeLogoSchema) => LogoBlockRef) extends LogoBlock(schema,metaData,rawData){
  def executeHandler() = ???
}

class FileBasedLogoBlock;
class CompressedLogoBlock[A:ClassTag, B:ClassTag](schema: LogoSchema, metaData: LogoMetaData, rawEdge:Seq[B], crystals:Seq[(String,List[B])], rawAttr:A)




//TODO finish below

/**
  * Basic LogoBlock Class for UnlabeledPatternMatching
  * @param schema schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData for the block
  */
abstract class PatternLogoBlock[A:ClassTag](schema:LogoSchema, metaData: LogoMetaData, rawData:A) extends LogoBlock(schema, metaData, rawData) {

  //TODO testing required for below, this place needs further optimization
  def buildIndex(schema:KeyValueLogoSchema) = {
    val rawData = assemble()
    val keys = schema.keys.toSet



    if (keys.size == 1){

//      MapBuilder.fromListToMapLongFast(rawData.map(_.pattern),keys,keys.toSeq)

      MapBuilder.fromListToMapLongFastCompact(rawData.map(_.pattern),keys,keys.toSeq)

    } else if (keys.size == 2){

//      MapBuilder.fromListToMapLongFast(rawData.map(_.pattern),keys,keys.toSeq)

      MapBuilder.fromListToMapLongFastCompact(rawData.map(_.pattern),keys,keys.toSeq)

    } else{
      null.asInstanceOf[mutable.LongMap[CompactPatternList]]
    }



  }

  //sub class needs to over-write this method
  def iterator():Iterator[PatternInstance]
  def enumerateIterator():Iterator[PatternInstance]

  def assemble():Seq[PatternInstance] = iterator().toList

  def toKeyValueLogoBlock(key:Set[Int]):KeyValuePatternLogoBlock = {

    val keyValueSchema = new KeyValueLogoSchema(schema,key)
    val keyValueRawData = buildIndex(keyValueSchema)
    new KeyValuePatternLogoBlock(
      keyValueSchema,
      metaData,
      keyValueRawData
    )
  }

  def toConcreteLogoBlock:ConcretePatternLogoBlock = {
    new ConcretePatternLogoBlock(
      schema,
      metaData,
      assemble()
    )
  }

  def toFilteringLogoBlock(f:PatternInstance => Boolean):FilteringPatternLogoBlock[A] = {
    new FilteringPatternLogoBlock(this,f)
  }

  override def toString: String = {
    s"schema:\n${schema.toString}\nmetaData:${metaData.toString}\nrawData:${rawData.toString()}"
  }
}

/**
  * Basic LogoBlock Class in which pattern is concretified.
  * @param schema schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData for the block, here we assume the nodeIds are represented using Int.
  */
class ConcretePatternLogoBlock(schema:LogoSchema, metaData: LogoMetaData, rawData:Seq[PatternInstance]) extends PatternLogoBlock(schema,metaData,rawData){
  override def assemble(): Seq[PatternInstance] = rawData
  override def iterator(): Iterator[PatternInstance] = rawData.iterator
  override def enumerateIterator(): Iterator[PatternInstance] = iterator()

}

class CompactConcretePatternLogoBlock(schema:LogoSchema, metaData: LogoMetaData, rawData:CompactPatternList) extends PatternLogoBlock(schema,metaData,rawData){
  override def assemble(): Seq[PatternInstance] = rawData.iterator.toList
  override def iterator(): Iterator[PatternInstance] = rawData.iterator
  override def enumerateIterator(): Iterator[PatternInstance] = iterator()
}

/**
  * Basic LogoBlock Class representing edges.
  * @param schema schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData for the block, here we assume the nodeIds are represented using Int.
  */
class EdgePatternLogoBlock(schema:LogoSchema, metaData: LogoMetaData, rawData:Seq[PatternInstance]) extends ConcretePatternLogoBlock(schema,metaData,rawData){

}


//TODO test required
/**
  * LogoBlock which can be used as a key-value map.
  * @param schema schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData for the block, here we assume the nodeIds are represented using Int.
  */
class KeyValuePatternLogoBlock(schema:KeyValueLogoSchema, metaData: LogoMetaData, rawData:mutable.LongMap[CompactPatternList]) extends PatternLogoBlock(schema,metaData,rawData){
  def valueMapping(keyMapping:KeyMapping)= KeyMapping(schema.valueKeyMapping(keyMapping))

  //get the values from the key in KeyValuePatternLogoBlock
  def getValue(key:KeyPatternInstance) ={

    val resRaw = rawData.getOrElse(key.node,null)
    val res = resRaw match {
      case null => None
      case _ => Some(resRaw)
    }



//
//    if (rawData.contains(key)){
//      print(s"contain key $key")
//      val result = rawData(key)
//      print(result)
//    }

    res
  }


  //TODO this part is wrong, it is only just a temporary fix
  override def iterator() = null
//    rawData.toSeq.flatMap(f => f._2).iterator

  override def enumerateIterator(): Iterator[PatternInstance] = null
//    iterator()
}


//TODO: test filtering
class FilteringPatternLogoBlock[A:ClassTag](var logoBlock: PatternLogoBlock[A], var f:PatternInstance=>  Boolean) extends PatternLogoBlock(logoBlock.schema,logoBlock.metaData,logoBlock.rawData) {


  def toFilteringIterator(it:Iterator[PatternInstance]):Iterator[PatternInstance] = {
    new AbstractIterator[PatternInstance] {

      private var hd: PatternInstance = _

      final def hasNext: Boolean =  {
        do {
          if (!it.hasNext) return false
          hd = it.next()
        } while (!f(hd))
        true
      }

      final def next() = hd
    }
  }

  override def iterator(): Iterator[PatternInstance] =  toFilteringIterator(logoBlock.iterator())
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
  * @param schema composite schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData is other PatternLogoBlocks which assembled this block
  */
class CompositeTwoPatternLogoBlock(schema:PlannedTwoCompositeLogoSchema, metaData:LogoMetaData, rawData:Seq[PatternLogoBlock[_]]) extends PatternLogoBlock(schema, metaData, rawData){

  lazy val coreBlock = schema.getCoreBlock(rawData)
  lazy val leafsBlock = schema.getLeafBlock(rawData)
  lazy val coreLeafJoints = schema.getCoreLeafJoins()
  lazy val coreKeyMapping = schema.coreKeyMapping
  lazy val leafValueMapping = schema.leafBlockSchema.valueKeyMapping(schema.leafKeyMapping)
  lazy val valueMapping = leafsBlock.valueMapping(schema.leafKeyMapping)

  //TODO testing required


  def arrayToIterator(array:Array[ValuePatternInstance]):Iterator[ValuePatternInstance] = {

    val res = new AbstractIterator[ValuePatternInstance] {

      private var cur = 0
      private var end  = array.length
      private var curEle:ValuePatternInstance = _


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
  def genereateLeafsNode(coreInstance:PatternInstance):Iterator[ValuePatternInstance] = {

//    val optValues = leafsBlock.
//      getValue(coreInstance.subPatterns(coreLeafJoints.coreJoints).toKeyPatternInstance())

    val optValues = leafsBlock.
            getValue(coreInstance.toSubKeyPattern(coreLeafJoints.coreJoints,coreJointsSeq))


      optValues match {
        case Some(values) =>   values.iterator   //arrayToIterator(values)
        case _ => null
      }
  }


  @transient lazy val totalNodes = (coreKeyMapping.values ++ valueMapping.values).toSeq.max + 1

  //assmeble the core and leafs instance into a single instance
  def assembleCoreAndLeafInstance(coreInstance:PatternInstance, leafInstanceNode:ValuePatternInstance):PatternInstance = {

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

//TODO:finish this place
  class FastEnumerateIterator extends Iterator[PatternInstance]{

    require(coreBlock.isInstanceOf[CompositeTwoPatternLogoBlock])
    val _coreBlock = coreBlock.asInstanceOf[CompositeTwoPatternLogoBlock]
    val oldCoreBlock = _coreBlock.coreBlock
    val oldLeafBlock = _coreBlock.leafsBlock
    val curLeafBlock = leafsBlock


  def isFeasible() ={

  }

    val oldCoreIterator:Iterator[PatternInstance] = oldCoreBlock.enumerateIterator()
    val oldLeafIterator:Iterator[PatternInstance] = null
    var leafsIterator:Iterator[PatternInstance] = null


    var currentCore:PatternInstance = null
    val array = Array.fill(totalNodes)(0)
    val currentPattern:EnumeratePatternInstance = new EnumeratePatternInstance(array)


    def genereateLeafsNodeWithBlock(coreInstance:PatternInstance, block:KeyValuePatternLogoBlock):Iterator[PatternInstance] = {

      //    val optValues = leafsBlock.
      //      getValue(coreInstance.subPatterns(coreLeafJoints.coreJoints).toKeyPatternInstance())

      val optValues = block.
        getValue(coreInstance.toSubKeyPattern(coreLeafJoints.coreJoints,coreJointsSeq))


      optValues match {
        case Some(values) => values.iterator
        case _ => null
      }
    }


    override def hasNext: Boolean = ???

    override def next(): PatternInstance = ???
  }


  //the iterator to iterate through the pattern, core is iterated but leaf are concrefied but treat as a iterator for convinence.
  class patternIterator extends Iterator[PatternInstance]{

    var leafsIterator:Iterator[ValuePatternInstance] = _
    val coreIterator:Iterator[PatternInstance] = coreBlock.iterator()
    var currentCore:PatternInstance = _

    override def hasNext: Boolean = {

      if (leafsIterator == null || !leafsIterator.hasNext){
          do {
            if (coreIterator.hasNext){
              currentCore = coreIterator.next()
            }else{
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
      assembleCoreAndLeafInstance(core,leafs)
    }
  }

  class EnumerateIterator extends Iterator[PatternInstance]{


    var leafsIterator:Iterator[ValuePatternInstance] = _
    val coreIterator:Iterator[PatternInstance] = coreBlock.enumerateIterator()
    var currentCore:PatternInstance = _
//    val TintBuffer = new TIntArrayList(totalNodes)
    val array = Array.fill(totalNodes)(0)
    val currentPattern:EnumeratePatternInstance = new EnumeratePatternInstance(array)
    val coreMapping = coreKeyMapping.keyMapping.toArray

    val coreMapping1 = coreMapping.map(_._1)
    val coreMapping2 = coreMapping.map(_._2)



    val valueKeyMapping = valueMapping.keyMapping.toArray

    val valueKeyMapping1 = valueKeyMapping.map(_._1)
    val valueKeyMapping2 = valueKeyMapping.map(_._2)

    val updateArray = coreMapping ++ valueKeyMapping
    val coreLen = coreMapping.length
    val valueLen = valueKeyMapping.length




    override def hasNext: Boolean = {

      if (leafsIterator == null || !leafsIterator.hasNext){
        do {
          if (coreIterator.hasNext){
            currentCore = coreIterator.next()


            var i = 0

            while (i < coreLen) {

              array.update(coreMapping2(i),currentCore.pattern(coreMapping1(i)))
              i += 1
            }


          }else{
            return false
          }
          leafsIterator = genereateLeafsNode(currentCore)

//          //TODO:experiment test
//          if (valueMappingSize == 1 && leafsIterator != null){
//            leafsIterator = leafsIterator.filter(p => !NonJoinCore.contains(p.pattern(0)))
//          }
        } while (leafsIterator == null)
      }

      return true
    }



    val valueMappingSize = valueMapping.keyMapping.size

    override def next(): PatternInstance = {
      val leafs = leafsIterator.next()
      val core = currentCore

      if (valueMappingSize == 0){
        core
      }else {
        var i = 0

        while (i < valueLen) {
          array.update(valueKeyMapping2(i),leafs.getValue(valueKeyMapping1(i)))
          i += 1
        }

        currentPattern
    }
  }

  }

  /**
    * generate a ConcretePatternLogoBlock
    */
  override def iterator() = new patternIterator
  override def enumerateIterator() = new EnumerateIterator

}



//TODO now we limit the implementation to that leaf and leaf must have intesection
/**
  * Composite LogoBlock Class for UnlabeledPatternMatching, which is assembled using multiple basic PatternLogoBlock.
  * In this specific class, there are only two blocks to build the new composite block.s
  * @param schema composite schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData is other PatternLogoBlocks which assembled this block
  */
class CompositeThreePatternLogoBlock(schema:PlannedThreeCompositeLogoSchema, metaData:LogoMetaData, rawData:Seq[PatternLogoBlock[_]]) extends PatternLogoBlock(schema, metaData, rawData){

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

  val leftCoreJointsSeq  = leftCoreLeafJoints.coreJoints.toSeq.sorted
  val rightCoreJointsSeq  = rightCoreLeafJoints.coreJoints.toSeq.sorted

  //generate the leaf instance grow from this core, actually the Iterator is just a wrapper the leafs are concrefied
  //the intersection node is not included in the returned iterator.
  def genereateLeftLeafsNode(coreInstance:PatternInstance):Array[Int] = {

    val optValues = leftLeafsBlock.
      getValue(coreInstance.toSubKeyPattern(leftCoreLeafJoints.coreJoints,leftCoreJointsSeq))

    optValues match {
      case Some(values) => values.getRaw()
      case _ => null
    }
  }

  //generate the leaf instance grow from this core, actually the Iterator is just a wrapper the leafs are concrefied
  //the intersection node is not included in the returned iterator.
  def genereateRightLeafsNode(coreInstance:PatternInstance):Array[Int] = {

    val optValues = rightLeafsBlock.
      getValue(coreInstance.toSubKeyPattern(rightCoreLeafJoints.coreJoints,rightCoreJointsSeq))

    optValues match {
      case Some(values) => values.getRaw()
      case _ => null
    }
  }


  @transient lazy val totalNodes = (coreKeyMapping.values ++ leftvalueMapping.values ++ rightvalueMapping.values).toSeq.max + 1

  //assmeble the core and leafs instance into a single instance
  def assembleCoreAndLeafInstance(coreInstance:PatternInstance, leftLeafInstanceNode:ValuePatternInstance):PatternInstance = {

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
  def generateIntersectionIterator(leftIterator:Array[Int], rightIterator:Array[Int]):Iterator[ValuePatternInstance] ={

    if (leftIterator == null || !(leftIterator.size != 0)){
      return new AbstractIterator[ValuePatternInstance] {override def hasNext: Boolean =
      {
        false
      }
        override def next(): ValuePatternInstance = {
          null
        }
      }
    }

    if (rightIterator == null || !(rightIterator.size != 0)){
      return new AbstractIterator[ValuePatternInstance] {override def hasNext: Boolean =
      {
        false
      }
        override def next(): ValuePatternInstance = {
          null
        }
      }
    }

    new AbstractIterator[ValuePatternInstance] {

      var nextEle:OneValuePatternInstance = new OneValuePatternInstance(0)
      var leftArray = leftIterator
      var rightArray = rightIterator
      var uCur = 0
      var vCur = 0
      val uD = leftArray.size
      val vD = rightArray.size

      final override def hasNext: Boolean = {

        while ( {
          (uCur < uD) && (vCur < vD)
        }) {

          if (leftArray(uCur) < rightArray(vCur)) uCur += 1
          else if (leftArray(uCur) > rightArray(vCur)) vCur += 1
          else {
            nextEle.node1 = leftArray(uCur)
            uCur += 1
            vCur += 1
            return true
          }
        }

        return false
      }


      final override def next(): ValuePatternInstance = {
        nextEle
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
  class patternIterator extends Iterator[PatternInstance]{

    var leftLeafsIterator:Array[Int] = _
    var rightLeafsIterator:Array[Int] = _
    var intersectIterator:Iterator[ValuePatternInstance] = _
    val coreIterator:Iterator[PatternInstance] = coreBlock.iterator()
    var currentCore:PatternInstance = _

    override def hasNext: Boolean = {

      if (intersectIterator == null || !intersectIterator.hasNext ){
        do {
          if (coreIterator.hasNext){
            currentCore = coreIterator.next()
          }else{
            return false
          }
          leftLeafsIterator = genereateLeftLeafsNode(currentCore)
          rightLeafsIterator = genereateRightLeafsNode(currentCore)
          intersectIterator = generateIntersectionIterator(leftLeafsIterator,rightLeafsIterator)

        } while (!intersectIterator.hasNext)
      }

      return true
    }

    override def next(): PatternInstance = {
      val leafs = intersectIterator.next()
      val core = currentCore
      assembleCoreAndLeafInstance(core,leafs)
    }
  }

  class EnumerateIterator extends Iterator[PatternInstance]{

    var leftLeafsIterator:Array[Int] = _
    var rightLeafsIterator:Array[Int] = _
    var intersectIterator:Iterator[ValuePatternInstance] = _
    val coreIterator:Iterator[PatternInstance] = coreBlock.enumerateIterator()
    var currentCore:PatternInstance = _
    //    val TintBuffer = new TIntArrayList(totalNodes)
    val array = Array.fill(totalNodes)(0)
    val currentPattern:EnumeratePatternInstance = new EnumeratePatternInstance(array)
    val coreMapping = coreKeyMapping.keyMapping.toArray

    val coreMapping1 = coreMapping.map(_._1)
    val coreMapping2 = coreMapping.map(_._2)

    val valueKeyMapping = leftvalueMapping.keyMapping.toArray

    val valueKeyMapping1 = valueKeyMapping.map(_._1)
    val valueKeyMapping2 = valueKeyMapping.map(_._2)

    val updateArray = coreMapping ++ valueKeyMapping
    val coreLen = coreMapping.length
    val valueLen = valueKeyMapping.length

    override def hasNext: Boolean = {

//      if (intersectIterator.hasNext){
//        return true
//      }

      var hasNext1 = false
      if (intersectIterator == null || !intersectIterator.hasNext){
        do {
          if (coreIterator.hasNext){
            currentCore = coreIterator.next()
//            var i = 0

//            while (i < coreLen) {
//              val temp = coreMapping(i)
//              array.update(temp._2,currentCore.pattern(temp._1))
//              i += 1
//            }

            var i = 0
            while (i < coreLen) {
              //                            val temp = coreMapping(i)
              array.update(coreMapping2(i),currentCore.pattern(coreMapping1(i)))
              i += 1
            }
          }else{
            return false
          }
          leftLeafsIterator = genereateLeftLeafsNode(currentCore)
          rightLeafsIterator = genereateRightLeafsNode(currentCore)
          intersectIterator = generateIntersectionIterator(leftLeafsIterator,rightLeafsIterator)


//          if (intersectIterator.hasNext){
//           var i = 0
//            while (i < coreLen) {
////                            val temp = coreMapping(i)
//                            array.update(coreMapping2(i),currentCore.pattern(coreMapping1(i)))
//                            i += 1
//                          }
//          }

//          hasNext1 =

//          val x = 1
        } while (!intersectIterator.hasNext)
      }

      return true
    }




    override def next(): PatternInstance = {
      val leafs = intersectIterator.next()
      val core = currentCore

      if (leftvalueMapping.keyMapping.size == 0){
        core
      }else {
        var i = 0

        while (i < valueLen) {
          val temp = valueKeyMapping(i)
          array.update(valueKeyMapping2(i),leafs.getValue(valueKeyMapping1(i)))
          i += 1
        }

        //        valueKeyMapping.foreach{
        //          f => array.update(f._2,leafs.pattern(f._1))
        //        }

        currentPattern
      }
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






