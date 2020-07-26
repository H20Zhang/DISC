package org.apache.spark.disc.execution.subtask.executor

import org.apache.spark.disc.catlog.Catalog.DataType
import org.apache.spark.disc.execution.hcube.TrieHCubeBlock
import org.apache.spark.disc.execution.subtask.{
  CachedLeapFrogJoinSubTask,
  LeapFrogJoinSubTask,
  TrieConstructedAttributeOrderInfo,
  TrieConstructedLeapFrogJoinSubTask
}
import org.apache.spark.disc.execution.subtask.utils.{ArraySegment, LRUCache}

import scala.collection.mutable

class CachedLeapFrogJoin(task: CachedLeapFrogJoinSubTask)
    extends LeapFrogJoin(task) {

  var keyAndValues = task.keyAndValues
  var cacheSizes = task.cacheSize
  var numBag = keyAndValues.size

  protected var partialLeapFrogIts = new Array[PartialLeapFrogJoin](numBag)
  protected var groupIterators = new Array[Iterator[Array[DataType]]](numBag)
  protected var groupPoses = new Array[(Array[Int], Array[Int])](numBag)
  protected var partialBindingsForPartalLFIts = new Array[ArraySegment](numBag)
  protected var lruCaches = new Array[IntArrayLRUCache](numBag)
  protected var lruCacheKeys = new Array[mutable.WrappedArray[DataType]](numBag)
  protected var lastGroupIdx = 0
  protected var lastGroupIterator: Iterator[Array[DataType]] = null
  protected var localPosToKeyPosMappings = new Array[Array[Int]](numBag)
  protected var localPosToValuePosMappings = new Array[Array[Int]](numBag)

  def initCacheLeapFrogJoin() = {
    //init newly added parameters
    keyAndValues = task.keyAndValues
    cacheSizes = task.cacheSize
    numBag = keyAndValues.size

    partialLeapFrogIts = new Array[PartialLeapFrogJoin](numBag)
    groupIterators = new Array[Iterator[Array[DataType]]](numBag)
    groupPoses = new Array[(Array[Int], Array[Int])](numBag)
    partialBindingsForPartalLFIts = new Array[ArraySegment](numBag)
    lruCaches = new Array[IntArrayLRUCache](numBag)
    lruCacheKeys = new Array[mutable.WrappedArray[DataType]](numBag)
    localPosToKeyPosMappings = new Array[Array[Int]](numBag)
    localPosToValuePosMappings = new Array[Array[Int]](numBag)

    //init relations
    initRelations()

    //prepare for information needed for init groupIts
    pareparePartialLeapFrogInfo()

    //init LRUs
    initLRUs()

    //init group iterators
    initGroupIts()

    lastGroupIdx = numBag - 1
    lastGroupIterator = groupIterators(lastGroupIdx)
  }

  override def init(): Unit = {}

  def initLRUs(): Unit = {

    var i = 0
    while (i < numBag) {

      lruCaches(i) = new IntArrayLRUCache(cacheSizes(i))
      lruCacheKeys(i) =
        mutable.WrappedArray.make[Long](new Array[Long](groupPoses(i)._1.size))

      i += 1
    }
  }

  def pareparePartialLeapFrogInfo(): Unit = {

    //prepare the info for init PartialLeapFrog
    var i = 0
    while (i < numBag) {

      val iThKeyAndValues = keyAndValues(i)
      val relatedAttrOrders = iThKeyAndValues._1 ++ iThKeyAndValues._2
      val relatedSchemas = schemas.zipWithIndex.filter {
        case (schema, idx) =>
          schema.attrIDs.diff(relatedAttrOrders) != schema.attrIDs
      }

      println(
        s"partialLeapFrog-${i}, relatedSchemas:${relatedSchemas}, iThKeyAndValues:${(iThKeyAndValues._1.toSeq, iThKeyAndValues._2.toSeq)}"
      )

      val relatedTriHCubeBlocks = relatedSchemas.map {
        case (schema, idx) =>
          val relatedTrie = tries(idx)
          val block = TrieHCubeBlock(schema, Array(), relatedTrie)
          block
      }

      val trieConstructedAttrOrderInfo = TrieConstructedAttributeOrderInfo(
        relatedAttrOrders
      )
      val trieConstructedSubTask = new TrieConstructedLeapFrogJoinSubTask(
        task.shareVector,
        relatedTriHCubeBlocks,
        trieConstructedAttrOrderInfo
      )

      //init Partial LeapFrog
      val partialLeapFrogJoin = new PartialLeapFrogJoin(trieConstructedSubTask)
      partialLeapFrogIts(i) = partialLeapFrogJoin

      //init keyMap and valueMap
      val localPosToKeyPosMapping =
        iThKeyAndValues._1.map(attrId => attrOrders.indexOf(attrId))
      localPosToKeyPosMappings(i) = localPosToKeyPosMapping

      val localPosToValuePosMapping = (iThKeyAndValues._1 ++ iThKeyAndValues._2)
        .map(attrId => attrOrders.indexOf(attrId))
      localPosToValuePosMappings(i) = localPosToValuePosMapping

      //init PartialBindings for the Partial LeapFrog

      val key = iThKeyAndValues._1.headOption
      val keySize = iThKeyAndValues._1.size
      val arrayForPartialBinding = new Array[DataType](keySize)

      if (key.nonEmpty) {
        partialBindingsForPartalLFIts(i) =
          new ArraySegment(arrayForPartialBinding, 0, keySize, keySize)
      } else {
        partialBindingsForPartalLFIts(i) = new ArraySegment(Array(), 0, 0, 0)
      }

      //init GroupPoses
      groupPoses(i) = (
        iThKeyAndValues._2,
        iThKeyAndValues._2.map(
          pos => relatedAttrOrders.indexOf(attrOrders(pos))
        )
      )

      i += 1
    }

  }

  def initGroupIts() = {
    //gradually init the group iterator for 0, 1, ..., n-th group
    var i = 0
    while (i < numBag) {
      fixGroupIts(i)
      i += 1
    }
  }

  protected def fixGroupIts(idx: Int): Unit = {
    if (idx == 0 && hasEnd != true) {
      //case: fix the iterator for first attribute, this will be fixed only once
      val it = constructedGroupIts(idx)

      if (it.hasNext) {
        groupIterators(idx) = it
        return
      } else {
        hasEnd = true
        return
      }
    }

    while (hasEnd != true) {
      //case: fix the iterator for the rest of the attributes
      val prevIdx = idx - 1
      val prevGroupIt = groupIterators(prevIdx)
      while (prevGroupIt.hasNext) {

        val nextBinding = prevGroupIt.next()

        //set the binding according to prevIts
        val localPosToValuePosMapping = localPosToValuePosMappings(prevIdx)
        val keySize = localPosToKeyPosMappings(prevIdx).size
        var i = keySize
        val bindingSize = nextBinding.size
        while (i < bindingSize) {
          binding(localPosToValuePosMapping(i)) = nextBinding(i)
          i += 1
        }

        //construct the groupIt for the idx
        val it = constructedGroupIts(idx)
        if (it.hasNext) {
          groupIterators(idx) = it
          return
        }
      }

      if (prevIdx == 0) {
        hasEnd = true
        return
      } else {
        fixGroupIts(prevIdx)
      }
    }

  }

  protected def constructedGroupIts(idx: Int): Iterator[Array[DataType]] = {

//    val partialBinding = partialBindingsForPartalLFIts(idx)
//    val localPosToKeyPosMapping = localPosToValuePosMappings(idx)
//
//    var i = 0
//    val prefixSize = partialBinding.size
//    while (i < prefixSize) {
//      partialBinding(i) = binding(localPosToKeyPosMapping(i))
//      i += 1
//    }
//
//    val partialLeapFrogIt = partialLeapFrogIts(idx)
//    partialLeapFrogIt.setPrefix(partialBinding)
//    return partialLeapFrogIt

    val partialBinding = partialBindingsForPartalLFIts(idx)
    val localPosToKeyPosMapping = localPosToValuePosMappings(idx)

    var i = 0
    val prefixSize = partialBinding.size
    while (i < prefixSize) {
      partialBinding(i) = binding(localPosToKeyPosMapping(i))
      i += 1
    }

    if (idx > 1) {
      val cachedGroupContent = getGroupContent(idx, partialBinding)

      if (cachedGroupContent != null) {
        return new ArrayIt(cachedGroupContent)
      } else {
        val partialLeapFrogIt = partialLeapFrogIts(idx)
        partialLeapFrogIt.setPrefix(partialBinding)
        val contentToCache = partialLeapFrogIt.toArray
        cacheGroupContent(idx, partialBinding, contentToCache)
        return new ArrayIt(contentToCache)
      }
    } else {
      val partialLeapFrogIt = partialLeapFrogIts(idx)
      partialLeapFrogIt.setPrefix(partialBinding)
      return partialLeapFrogIt
    }
  }

  class ArrayIt(content: Array[Array[DataType]])
      extends Iterator[Array[DataType]] {
    var start = 0
    var end = content.size

    override def hasNext: Boolean = {
      start < end
    }

    override def next(): Array[DataType] = {
      val arr = content(start)
      start += 1
      arr
    }

    override def size: Int = {
      content.size
    }
  }

  protected def cacheGroupContent(idx: Int,
                                  key: ArraySegment,
                                  content: Array[Array[DataType]]): Unit = {

    val keySize = groupPoses(idx)._1.size
    val lruKey = mutable.WrappedArray.make[Long](new Array[Long](keySize))
    var i = 0
    while (i < keySize) {
      lruKey(i) = key(i)
      i += 1
    }

    lruCaches(idx).put(lruKey, content)
  }

  protected def getGroupContent(idx: Int,
                                key: ArraySegment): Array[Array[DataType]] = {

    val lruKey = lruCacheKeys(idx)
    val keySize = lruKey.size
    var i = 0
    while (i < keySize) {
      lruKey(i) = key(i)
      i += 1
    }
    val content = lruCaches(idx).get(lruKey)
    content
  }

  override def hasNext: Boolean = {
    if (lastGroupIterator.hasNext) {
      return true
    } else {
      fixGroupIts(lastGroupIdx)
      lastGroupIterator = groupIterators(lastGroupIdx)
      return !hasEnd
    }
  }

  override def next(): Array[DataType] = {

    //set the binding according to prevIts
    val groupPos = groupPoses(lastGroupIdx)
    val outerPos = groupPos._1
    val innerPos = groupPos._2
    var i = 0
    val nextBinding = lastGroupIterator.next()
    val groupPosSize = outerPos.size
    while (i < groupPosSize) {
      binding(outerPos(i)) = nextBinding(innerPos(i))
      i += 1
    }

    binding
  }

  override def longSize(): Long = {

    var count = 0L
    while (!hasEnd) {
      count += groupIterators(lastGroupIdx).size
      fixGroupIts(lastGroupIdx)
    }

    count

  }

  override protected def initIterators(): Unit = {
    throw new Exception("initIterators() is not allowed")
  }

  override protected def fixIterators(idx: Int): Unit = {
    throw new Exception("fixIterators() is not allowed")
  }

  override protected def constructIthIterator(idx: Int): Iterator[DataType] = {
    throw new Exception("constructIthIterator() is not allowed")
  }

}

class PartialLeapFrogJoin(trieTask: TrieConstructedLeapFrogJoinSubTask)
    extends TrieConstructedLeapFrogJoin(trieTask) {

  var startPos = 0

  def setPrefix(prefix: ArraySegment) = {
    firstInitialized = false
    hasEnd = false
    var i = 0
    val prefixSize = prefix.size
    startPos = prefixSize
    while (i < prefixSize) {
      binding(i) = prefix(i)
      i += 1
    }

    i = startPos
    while (i < attrSize) {
      fixIterators(i)
      i += 1
    }

    lastIterator = unaryIterators(lastIdx)
  }

  override def init(): Unit = {
    tries = trieTask.tries.map(_.asInstanceOf[TrieHCubeBlock].content).toArray
  }

  override protected def fixIterators(idx: Int): Unit = {

    if (idx == startPos && firstInitialized != true) {

      firstInitialized = true
      //case: fix the iterator for first attribute, this will be fixed only once
      val it = constructIthIterator(idx)

      if (it.hasNext) {
        unaryIterators(idx) = it
        return
      } else {
        hasEnd = true
        return
      }

    } else if (idx == startPos && firstInitialized == true) {
      hasEnd = true
      return
    }

    while (hasEnd != true) {
      //case: fix the iterator for the rest of the attributes
      val prevIdx = idx - 1
      val prevIterator = unaryIterators(prevIdx)
      while (prevIterator.hasNext) {
        binding(prevIdx) = prevIterator.next()
        val it = constructIthIterator(idx)
        if (it.hasNext) {
          unaryIterators(idx) = it
          return
        }
      }

      if (prevIdx == startPos) {
        hasEnd = true
        return
      } else {
        fixIterators(prevIdx)
      }
    }

  }

}

class IntArrayLRUCache(cacheSize: Int = 10000)
    extends LRUCache[mutable.WrappedArray[DataType], Array[Array[DataType]]](
      cacheSize
    ) {
  def apply(key: mutable.WrappedArray[DataType]): Array[Array[DataType]] =
    get(key)
  def update(key: mutable.WrappedArray[DataType],
             value: Array[Array[DataType]]): Unit =
    put(key, value)
}
