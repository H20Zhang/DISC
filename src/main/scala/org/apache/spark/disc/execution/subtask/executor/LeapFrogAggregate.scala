package org.apache.spark.disc.execution.subtask.executor

import java.util

import it.unimi.dsi.fastutil.longs
import it.unimi.dsi.fastutil.longs.LongArrayList
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap
import org.apache.spark.adj.database.{Catalog, RelationSchema}
import org.apache.spark.adj.database.Catalog.{AttributeID, DataType}
import org.apache.spark.adj.execution.hcube.TrieHCubeBlock
import org.apache.spark.adj.execution.subtask.{
  TrieConstructedAttributeOrderInfo,
  TrieConstructedLeapFrogJoinSubTask
}
import org.apache.spark.adj.execution.subtask.executor.{
  LongSizeIterator,
  PartialLeapFrogJoin,
  TrieConstructedLeapFrogJoin
}
import org.apache.spark.adj.execution.subtask.utils.{
  ArraySegment,
  LRUCache,
  Trie
}
import org.apache.spark.disc.DISCConf
import org.apache.spark.disc.execution.subtask.{
  EagerTableSubInfo,
  LazyTableSubInfo,
  LeapFrogAggregateInfo,
  LeapFrogAggregateSubTask
}
import org.apache.spark.disc.util.{Counter, Graph}

import scala.collection.mutable

class LeapFrogAggregate(aggTask: LeapFrogAggregateSubTask) {

  val info = aggTask.info.asInstanceOf[LeapFrogAggregateInfo]
  val schemas = aggTask.tries.map(_.asInstanceOf[TrieHCubeBlock].schema)
  val triBlocks = aggTask.tries.map(_.asInstanceOf[TrieHCubeBlock])
  var schemaToTrieMap = schemas.zip(triBlocks).toMap

  //initialization needed
  var eagerTables: Array[BindingAssociatedEagerTable] = _
  var lazyTables: Array[AbstractBindingAssociatedLazyTable] = _
  var outputTable: BindingAssociatedOutputTable = _
  var lf: TrieConstructedLeapFrogJoin = _

  def init() = {

    //construct leapfrog iterator of instance of hyperedge
    lf = constructLF()

    //construct eagerTables
    eagerTables = constructEagerTables()

    //construct lazyTables
    lazyTables = constructLazyTables()

    //construct outputTable
    outputTable = constructOutputTable()

  }

  //use TrieConstructedLeapFrogJoin as iterator
  def constructLF() = {

    //prepare trieConstructedAttributeOrderInfo
    val attrIdsOrder = info.edgeAttrIdsOrder
    val trieConstructedAttributeOrderInfo = TrieConstructedAttributeOrderInfo(
      attrIdsOrder
    )

    //prepare trieConstructedLeapFrogJoinSubTask
    val relatedSchemas = info.edges
    val relatedTries = relatedSchemas.map(schemaToTrieMap)
    val subTask = new TrieConstructedLeapFrogJoinSubTask(
      aggTask.shareVector,
      relatedTries,
      trieConstructedAttributeOrderInfo
    )

    subTask.execute()
  }

  def constructEagerTables() = {
    val eagerTableInfos = info.eagerTableInfos
    eagerTableInfos.map { eagerTableSubInfo =>
      val eagerTable =
        new BindingAssociatedEagerTable(
          schemaToTrieMap(eagerTableSubInfo.schema).content
        )
      eagerTable.init(
        lf.getBinding(),
        info.edgeAttrIdsOrder,
        eagerTableSubInfo.coreAttrIdsOrder
      )
      eagerTable
    }.toArray
  }

  def constructLazyTables() = {
    val lazyTableInfos = info.lazyTableInfos

    lazyTableInfos.map { lazyTableInfo =>
      var lazyTable: AbstractBindingAssociatedLazyTable = null

      val lazyTableCore = lazyTableInfo.coreAttrIds
      val lazyTableEdge = lazyTableInfo.schemas
      val filteredEdge = lazyTableEdge
        .filter(schema => schema.attrIDs.diff(lazyTableCore).isEmpty)
        .map(schema => (schema.attrIDs(0), schema.attrIDs(1)))
        .flatMap(f => Iterable(f, f.swap))
        .distinct

      val coreInducedGraph = new Graph(lazyTableCore, filteredEdge)
      val isCoreConnected = coreInducedGraph.isConnected()
      val isOrderCompetiable = info.edgeAttrIdsOrder
        .slice(0, lazyTableCore.size)
        .containsSlice(lazyTableCore)

      if (isCoreConnected && isOrderCompetiable) {
        lazyTable = new ConsecutiveNoEagerBindingAssociatedLazyTable(
          lazyTableInfo,
          schemaToTrieMap
        )
      } else if (lazyTableInfo.eagerTableInfos.isEmpty) {
        lazyTable =
          new NoEagerBindingAssociatedLazyTable(lazyTableInfo, schemaToTrieMap)
      } else {
        lazyTable =
          new BindingAssociatedLazyTable(lazyTableInfo, schemaToTrieMap)
      }

      lazyTable.init(
        lf.getBinding(),
        info.edgeAttrIdsOrder,
        lazyTableInfo.coreAttrIds
      )

      lazyTable
    }.toArray
  }

  def constructOutputTable(): BindingAssociatedOutputTable = {
    val outputTable = new BindingAssociatedOutputTable()
    outputTable.init(
      lf.getBinding(),
      info.edgeAttrIdsOrder,
      info.coreAttrIds.toArray
    )

    outputTable
  }

  def aggregate(): Array[Array[DataType]] = {

    val lazyTableNum = lazyTables.size
    val eagerTableNum = eagerTables.size
    var i = 0

    while (lf.hasNext) {
      lf.next()
      var C = 1l
      i = 0
      while (i < lazyTableNum) {
        C *= lazyTables(i).getCount()
        i += 1
      }
      i = 0
      while (i < eagerTableNum) {
        C *= eagerTables(i).getCount()
        i += 1
      }
      outputTable.increment(C)
    }

    outputTable.toArrays()
  }
}

abstract class BindingAssociatedCountTable {
  def getCount(): DataType
}

class BindingAssociatedEagerTable(trie: Trie)
    extends BindingAssociatedCountTable {

  //initialization required
  var outerBinding: Array[DataType] = _
  var innerBinding: Array[DataType] = _
  var innerBindingKey: ArraySegment = _
  var outerBindingToInnerBindingArray: Array[AttributeID] = _

  val outputArraySegement: ArraySegment = ArraySegment.newEmptyArraySegment()

  lazy val inputBindingSize = innerBinding.size

  def init(outerBinding: Array[DataType],
           outerBindingSchema: Array[AttributeID],
           innerBindingSchema: Array[AttributeID]): Unit = {

    this.outerBinding = outerBinding
    innerBinding = new Array[DataType](innerBindingSchema.size)
    innerBindingKey = ArraySegment(innerBinding)
    outerBindingToInnerBindingArray = innerBindingSchema
      .map(attrId => outerBindingSchema.indexOf(attrId))
  }

  override def getCount(): DataType = {

    //update the input binding
    var i = 0

    while (i < inputBindingSize) {
      innerBinding(i) = outerBinding(outerBindingToInnerBindingArray(i))
      i += 1
    }

    //query the trie to get the count value
    trie.nextLevel(innerBindingKey, outputArraySegement)
    if (outputArraySegement.size == 0) {
      0
    } else {
      outputArraySegement(0)
    }
  }
}

abstract class AbstractBindingAssociatedLazyTable(
  lazyTableSubInfo: LazyTableSubInfo,
  schemaToTrieMap: Map[RelationSchema, TrieHCubeBlock]
) extends BindingAssociatedCountTable {

  //binding conversion related variable
  var outerBinding: Array[DataType] = _
  var innerBinding: Array[DataType] = _
  var innerBindingKey: ArraySegment = _
  var outerBindingToInnerBindingArray: Array[AttributeID] = _

  //misc variable
  var partialLF: PartialLeapFrogJoin = _
  var eagerTables: Array[BindingAssociatedEagerTable] = _

  val outputValues: ArraySegment = ArraySegment.newEmptyArraySegment()
  lazy val innerBindingSize = innerBinding.size
  lazy val eagerTableSize = eagerTables.size

  var lruCache: ArrayLongLRUCache = _
  val lruKey: LongArrayList = new LongArrayList()

  def init(outerBinding: Array[DataType],
           outerBindingSchema: Array[AttributeID],
           innerBindingSchema: Array[AttributeID]) = {

    this.outerBinding = outerBinding

    innerBinding = new Array[DataType](innerBindingSchema.size)
    innerBindingKey = ArraySegment(innerBinding)
    outerBindingToInnerBindingArray =
      innerBindingSchema.map(attrId => outerBindingSchema.indexOf(attrId))

    partialLF = constructPartialLF()
    eagerTables = constructEagerTables()

    lruCache = new ArrayLongLRUCache()

    var i = 0; var size = outerBindingToInnerBindingArray.size
    while (i < size) {
      lruKey.add(0)
      i += 1
    }
    lruKey.trim(size)
  }

  def constructPartialLF(): PartialLeapFrogJoin = {
    //prepare trieConstructedLeapFrogJoinInfo
    val attrsIdOrder = lazyTableSubInfo.edgeAttrIdsOrder
    val trieConstructedLeapFrogJoinInfo = TrieConstructedAttributeOrderInfo(
      attrsIdOrder
    )

    //prepare trieConstructedLeapFrogJoinSubTask
    val relatedSchemas = lazyTableSubInfo.schemas
      .diff(lazyTableSubInfo.eagerTableInfos.map(_.schema))
    val relatedTries = relatedSchemas.map(schemaToTrieMap)
    val subTask = new TrieConstructedLeapFrogJoinSubTask(
      null,
      relatedTries,
      trieConstructedLeapFrogJoinInfo
    )

    //construct partialLeapFrogJoin
    val it = new PartialLeapFrogJoin(subTask)

    it
  }

  def constructEagerTables(): Array[BindingAssociatedEagerTable] = {
    val eagerTableInfos = lazyTableSubInfo.eagerTableInfos
    eagerTableInfos.map { eagerTableInfo =>
      val eagerTable =
        new BindingAssociatedEagerTable(
          schemaToTrieMap(eagerTableInfo.schema).content
        )
      eagerTable.init(
        partialLF.getBinding(),
        lazyTableSubInfo.edgeAttrIdsOrder,
        eagerTableInfo.coreAttrIdsOrder
      )
      eagerTable
    }.toArray
  }
}

class BindingAssociatedLazyTable(
  info: LazyTableSubInfo,
  schemaToTrieMap: Map[RelationSchema, TrieHCubeBlock]
) extends AbstractBindingAssociatedLazyTable(info, schemaToTrieMap) {

  override def getCount(): Long = {
    //update the input binding
    var i = 0
    while (i < innerBindingSize) {
      innerBinding(i) = outerBinding(outerBindingToInnerBindingArray(i))
      lruKey.set(i, innerBinding(i))
      i += 1
    }

    var totalC = 0l

    if (lruCache.contain(lruKey)) {
      return lruCache.get(lruKey)
    }

    //compute Count Value Online
    partialLF.setPrefix(innerBindingKey)
    while (partialLF.hasNext) {
      partialLF.next()
      var C = 1l

      var i = 0
      while (i < eagerTableSize) {
        C *= eagerTables(i).getCount()
        i += 1
      }
      totalC += C
    }

    if (totalC > 5) {
      lruCache.put(lruKey.clone(), totalC)
    }

    totalC
  }
}

class NoEagerBindingAssociatedLazyTable(
  info: LazyTableSubInfo,
  schemaToTrieMap: Map[RelationSchema, TrieHCubeBlock]
) extends AbstractBindingAssociatedLazyTable(info, schemaToTrieMap) {

  override def getCount(): Long = {
    //update the input binding
    var i = 0
    while (i < innerBindingSize) {
      val temp = outerBinding(outerBindingToInnerBindingArray(i))
      innerBinding(i) = temp
      lruKey.set(i, temp)
      i += 1
    }

    val value = lruCache.getOrDefault(lruKey, -1l)
    if (value != -1) {
      return value
    }

    //compute Count Value Online
    partialLF.setPrefix(innerBindingKey)
    val totalC = partialLF.longSize()
    if (totalC > 5) {
      lruCache.put(lruKey.clone(), totalC)
    }
    totalC
  }
}

class ConsecutiveNoEagerBindingAssociatedLazyTable(
  info: LazyTableSubInfo,
  schemaToTrieMap: Map[RelationSchema, TrieHCubeBlock]
) extends AbstractBindingAssociatedLazyTable(info, schemaToTrieMap) {

  lazy val lastInnerBinding: Array[DataType] =
    new Array[DataType](innerBindingSize)
  var lastCount: DataType = 0l

  override def getCount(): Long = {
    //update the input binding
    var i = innerBindingSize - 1
    var isSameAsLastBinding = true
    while (i >= 0) {
      val temp = outerBinding(outerBindingToInnerBindingArray(i))
      if (temp != lastInnerBinding(i)) {
        innerBinding(i) = temp
        lastInnerBinding(i) = temp
        isSameAsLastBinding = false
      }
      i -= 1
    }

    if (isSameAsLastBinding) {
      return lastCount
    }

    var totalC = 0l
    //compute Count Value Online
    partialLF.setPrefix(innerBindingKey)
    totalC = partialLF.longSize()
    lastCount = totalC

    totalC
  }
}

class BindingAssociatedOutputTable extends BindingAssociatedCountTable {

  //binding conversion related variable, initialization required
  var outerBinding: Array[DataType] = _
  var innerBinding: Array[DataType] = _
//  var innerBindingKey: mutable.WrappedArray[DataType] = _
  var innerBindingKey: LongArrayList = _
  var lastInnerBindingKey: LongArrayList = _
  var outerBindingToInnerBindingArray: Array[AttributeID] = _

  //hashTable that records count per inner binding

  //java.util.HashMap
//  val countTable: java.util.HashMap[mutable.WrappedArray[DataType], DataType] =
//    new java.util.HashMap[mutable.WrappedArray[DataType], DataType]()

  //fastutil.HashMap
//  val countTable: Object2LongOpenHashMap[mutable.WrappedArray[DataType]] =
//    new Object2LongOpenHashMap[mutable.WrappedArray[DataType]]()

  val countTable: Object2LongOpenHashMap[LongArrayList] =
    new Object2LongOpenHashMap[LongArrayList](10000, .5f)

  lazy val innerBindingSize = innerBinding.size

  var lastAggregateCount = 0l

  def init(outerBinding: Array[DataType],
           outerBindingSchema: Array[AttributeID],
           innerBindingSchema: Array[AttributeID]): Unit = {

    innerBinding = new Array[DataType](innerBindingSchema.size)
    this.outerBinding = outerBinding

    outerBindingToInnerBindingArray = innerBindingSchema
      .map(attrId => outerBindingSchema.indexOf(attrId))

    innerBindingKey = new LongArrayList()
    lastInnerBindingKey = new longs.LongArrayList()

    var i = 0; val size = innerBindingSize
    while (i < size) {
      innerBindingKey.add(0)
      lastInnerBindingKey.add(0)
      i += 1
    }

    innerBindingKey.trim(innerBindingSize)
    lastInnerBindingKey.trim(innerBindingSize)

  }

  override def getCount(): DataType = {
    finalizeAggregate()
    var i = 0
    while (i < innerBindingSize) {
      innerBindingKey.set(i, outerBinding(outerBindingToInnerBindingArray(i)))
      i += 1
    }

    countTable.getOrDefault(innerBindingKey, 0l)
  }

  def increment(value: DataType): Unit = {
    var i = 0
    var isSameAsLastBinding = true
    while (i < innerBindingSize) {
      val temp = outerBinding(outerBindingToInnerBindingArray(i))
      if (temp != lastInnerBindingKey.getLong(i)) {
        innerBindingKey.set(i, temp)
        isSameAsLastBinding = false
      }
      i += 1
    }

    if (isSameAsLastBinding) {
      lastAggregateCount += value
      return
    }

    finalizeAggregate()
    lastAggregateCount = value
  }

  def finalizeAggregate() = {
    val oldValue = countTable.getOrDefault(lastInnerBindingKey, 0l)
    val newValue = oldValue + lastAggregateCount

    if (oldValue == 0) {
      val key = lastInnerBindingKey.clone()
      countTable.put(key, newValue)
    } else {
      countTable.replace(lastInnerBindingKey, newValue)
    }

    var i = 0
    while (i < innerBindingSize) {

      if (innerBindingKey.getLong(i) != lastInnerBindingKey.getLong(i)) {
        lastInnerBindingKey.set(i, innerBindingKey.getLong(i))
      }
      i += 1
    }

    lastAggregateCount = 0l
  }

  def toArrays(): Array[Array[DataType]] = {

    finalizeAggregate()

    val outputArray = new Array[Array[DataType]](countTable.size())
    import scala.collection.JavaConversions._

    var j = 0
    for (entry <- countTable.entrySet()) {

      val key = entry.getKey
      val value = entry.getValue
      val oneTuple = new Array[DataType](key.size + 1)

      var i = 0
      val end = key.size

      while (i < end) {
        oneTuple(i) = key(i)
        i += 1
      }

      oneTuple(end) = value
      outputArray(j) = oneTuple

      j += 1
    }
    outputArray
  }
}

class ArrayLongLRUCache(cacheSize: Int = DISCConf.defaultConf().cacheSize)
    extends LRUCache[LongArrayList, DataType](cacheSize) {
  def apply(key: LongArrayList): DataType =
    get(key)
  def update(key: LongArrayList, value: DataType): Unit =
    put(key, value)
}

//class ArrayLongLRUCache(cacheSize: Int = DISCConf.defaultConf().cacheSize)
//    extends LRUCache[mutable.WrappedArray[Long], DataType](cacheSize) {
//  def apply(key: mutable.WrappedArray[Long]): DataType =
//    get(key)
//  def update(key: mutable.WrappedArray[Long], value: DataType): Unit =
//    put(key, value)
//}
//    var i = 0
//    while (i < innerBindingSize) {
//      innerBinding(i) = outerBinding(outerBindingToInnerBindingArray(i))
//      i += 1
//    }
//
//    val oldValue = countTable.getOrDefault(innerBindingKey, 0)
//    val newValue = oldValue + value
//
//    if (oldValue == 0) {
//      val key = mutable.WrappedArray
//        .make[DataType](new Array[DataType](innerBindingSize))
//      var i = 0
//      while (i < innerBindingSize) {
//        key(i) = innerBinding(i)
//        i += 1
//      }
//      countTable.put(key, newValue)
//    } else {
//      countTable.replace(innerBindingKey, newValue)
//    }

//  protected def setCache(content: DataType): Unit = {
//
//    val lruKey =
//      mutable.WrappedArray.make[DataType](new Array[DataType](innerBindingSize))
//
//    var i = 0
//    while (i < innerBindingSize) {
//      lruKey(i) = innerBinding(i)
//      i += 1
//    }
//
//    lruCache.put(lruKey, content)
//  }

//  protected def getCache(): DataType = {
//    var i = 0
//    while (i < innerBindingSize) {
//      lruKey(i) = innerBinding(i)
//      i += 1
//    }
//
//    if (lruCache.contain(lruKey)) {
//      lruCache.get(lruKey)
//    } else {
//      -1
//    }
//  }

//  protected def setCache(content: DataType): Unit = {
//    lruCache.put(lruKey.clone(), content)
//  }
//
//  protected def getCache(): DataType = {
//    if (lruCache.contain(lruKey)) {
//      lruCache.get(lruKey)
//    } else {
//      -1
//    }
//  }

//if (lazyTableNum == 0 && eagerTableNum == 0) {
//while (lf.hasNext) {
//lf.next()
//outputTable.increment(1l)
//}
//} else if (lazyTableNum == 0 && eagerTableNum != 0) {
//while (lf.hasNext) {
//lf.next()
//var C = 1l
//
//i = 0
//while (i < eagerTableNum) {
//C *= eagerTables(i).getCount()
//i += 1
//}
//
//outputTable.increment(C)
//}
//} else if (lazyTableNum != 0 && eagerTableNum == 0) {
//while (lf.hasNext) {
//lf.next()
//var C = 1l
//i = 0
//while (i < lazyTableNum) {
//C *= lazyTables(i).getCount()
//i += 1
//}
//outputTable.increment(C)
//}
//} else {
//if (lazyTableNum == 0 && eagerTableNum == 0) {
//
//while (lf.hasNext) {
//lf.next()
//outputTable.increment(1l)
//}
//} else if (lazyTableNum == 1 && eagerTableNum == 0) {
//
//val theLazyTable = lazyTables(0)
//while (lf.hasNext) {
//lf.next()
//outputTable.increment(theLazyTable.getCount())
//
//}
//} else if (lazyTableNum != 0 && eagerTableNum == 0) {
//
//while (lf.hasNext) {
//lf.next()
//var C = 1l
//i = 0
//while (i < lazyTableNum) {
//C *= lazyTables(i).getCount()
//i += 1
//}
//outputTable.increment(C)
//}
//} else if (lazyTableNum == 0 && eagerTableNum == 1) {
//
//while (lf.hasNext) {
//val theEagerTable = eagerTables(0)
//lf.next()
//outputTable.increment(theEagerTable.getCount())
//}
//} else if (lazyTableNum == 0 && eagerTableNum != 0) {
//
//while (lf.hasNext) {
//lf.next()
//var C = 1l
//i = 0
//while (i < eagerTableNum) {
//C *= eagerTables(i).getCount()
//i += 1
//}
//outputTable.increment(C)
//}
//} else {
//def debug(): Unit = {
//
//  //LF
//{
//  //prepare trieConstructedAttributeOrderInfo
//  val lazyTableInfo = info.lazyTableInfos(0)
//  val attrIdsOrder = info.globalAttrIdsOrder
//  //      val attrIdsOrder = info.edgeAttrIdsOrder
//  val trieConstructedAttributeOrderInfo = TrieConstructedAttributeOrderInfo(
//  attrIdsOrder
//  )
//
//  //prepare trieConstructedLeapFrogJoinSubTask
//  val relatedSchemas = info.edges ++ lazyTableInfo.schemas
//  val relatedTries = relatedSchemas.map(schemaToTrieMap)
//  val subTask = new TrieConstructedLeapFrogJoinSubTask(
//  aggTask.shareVector,
//  relatedTries,
//  trieConstructedAttributeOrderInfo
//  )
//
//  println(
//  s"attrIdOrder:${attrIdsOrder.toSeq}, relatedSchemas:${relatedSchemas}"
//  )
//
//  var totalC = 0l
//  val lf = subTask.execute()
//  while (lf.hasNext) {
//  lf.next()
//  totalC += 1
//}
//  println(s"lf.totalC:${totalC}")
//}
//
//  //partialLF
//{
//  val lf = { //prepare trieConstructedAttributeOrderInfo
//  val attrIdsOrder = info.edgeAttrIdsOrder
//  val trieConstructedAttributeOrderInfo =
//  TrieConstructedAttributeOrderInfo(attrIdsOrder)
//
//  //prepare trieConstructedLeapFrogJoinSubTask
//  val relatedSchemas = info.edges
//  val relatedTries = relatedSchemas.map(schemaToTrieMap)
//  val subTask = new TrieConstructedLeapFrogJoinSubTask(
//  aggTask.shareVector,
//  relatedTries,
//  trieConstructedAttributeOrderInfo
//  )
//
//  subTask.execute()
//}
//
//  val lazyTableInfo = info.lazyTableInfos(0)
//  val lazyCountTable =
//  new NoEagerBindingAssociatedLazyTable(lazyTableInfo, schemaToTrieMap)
//
//  lazyCountTable.init(
//  lf.getBinding(),
//  info.edgeAttrIdsOrder,
//  lazyTableInfo.coreAttrIds
//  )
//
//  var totalC = 0l
//  while (lf.hasNext) {
//  val intResult = lf.next()
//  //        println(s"i-${intResult.toSeq}")
//  totalC += lazyCountTable.getCount()
//}
//
//  println(s"totalC:${totalC}")
//
//}
//}
