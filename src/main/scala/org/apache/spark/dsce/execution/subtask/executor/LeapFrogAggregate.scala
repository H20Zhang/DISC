package org.apache.spark.dsce.execution.subtask.executor

import java.util

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
import org.apache.spark.dsce.execution.subtask.{
  EagerTableSubInfo,
  LazyTableSubInfo,
  LeapFrogAggregateInfo,
  LeapFrogAggregateSubTask
}

import scala.collection.mutable

//TODO: debug needed
class LeapFrogAggregate(aggTask: LeapFrogAggregateSubTask) {

  val info = aggTask.info.asInstanceOf[LeapFrogAggregateInfo]
  val schemas = aggTask.blocks.map(_.schema)
  val triBlocks = aggTask.tries.map(_.asInstanceOf[TrieHCubeBlock]).toArray
  val schemaToTrieMap = schemas.zip(triBlocks).toMap

  //initialization needed
  var eagerTables: Array[BindingAssociatedEagerTable] = _
  var lazyTables: Array[BindingAssociatedLazyTable] = _
  var outputTable: BindingAssociatedOutputTable = _
  var lf: TrieConstructedLeapFrogJoin = _

  def init() = {

    //construct leapfrog iterator of instance of hyperedge
    lf = constructHyperEdgeLeapFrog()

    //construct eagerTables
    eagerTables = constructEagerTables()

    //construct lazyTables
    lazyTables = constructLazyTables()

    //construct outputTable
    outputTable = constructOutputTable()

  }

  //use TrieConstructedLeapFrogJoin as iterator
  def constructHyperEdgeLeapFrog() = {

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
      val lazyTable =
        new BindingAssociatedLazyTable(lazyTableInfo, schemaToTrieMap)
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
//      outputTable.setCount(C + outputTable.getCount())
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

class BindingAssociatedLazyTable(
  info: LazyTableSubInfo,
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
  lazy val lruKey: mutable.WrappedArray[DataType] = mutable.WrappedArray
    .make[DataType](new Array[DataType](innerBindingKey.size))

  def init(outerBinding: Array[DataType],
           outerBindingSchema: Array[AttributeID],
           innerBindingSchema: Array[AttributeID]) = {

    this.outerBinding = outerBinding
    innerBinding = new Array[DataType](info.edgeAttrIdsOrder.size - 1)
    innerBindingKey = ArraySegment(innerBinding)
    outerBindingToInnerBindingArray =
      innerBindingSchema.map(attrId => outerBindingSchema.indexOf(attrId))

    partialLF = constructPatternIt()
    eagerTables = constructEagerTables()
    lruCache = new ArrayLongLRUCache()
  }

  def constructPatternIt(): PartialLeapFrogJoin = {
    //prepare trieConstructedLeapFrogJoinInfo
    val attrsIdOrder = info.edgeAttrIdsOrder
    val trieConstructedLeapFrogJoinInfo = TrieConstructedAttributeOrderInfo(
      attrsIdOrder
    )

    //prepare trieConstructedLeapFrogJoinSubTask
    val relatedSchemas = info.schemas
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
    val eagerTableInfos = info.eagerTableInfos
    eagerTableInfos.map { eagerTableInfo =>
      val eagerTable =
        new BindingAssociatedEagerTable(
          schemaToTrieMap(eagerTableInfo.schema).content
        )
      eagerTable.init(
        partialLF.getBinding(),
        info.edgeAttrIdsOrder,
        eagerTableInfo.coreAttrIdsOrder
      )
      eagerTable
    }.toArray
  }

  protected def setCache(content: DataType): Unit = {

    val lruKey =
      mutable.WrappedArray.make[DataType](new Array[DataType](innerBindingSize))

    var i = 0
    while (i < innerBindingSize) {
      lruKey(i) = innerBinding(i)
      i += 1
    }

    lruCache.put(lruKey, content)
  }

  protected def getCache(): DataType = {
    var i = 0
    while (i < innerBindingSize) {
      lruKey(i) = innerBinding(i)
      i += 1
    }

    if (lruCache.contain(lruKey)) {
      lruCache.get(lruKey)
    } else {
      -1
    }
  }

  override def getCount(): Long = {
    //update the input binding
    var i = 0
    while (i < innerBindingSize) {
      innerBinding(i) = outerBinding(outerBindingToInnerBindingArray(i))
      i += 1
    }

    val cachedResult = getCache()

    if (cachedResult != -1) {
      return cachedResult
    }

    partialLF.setPrefix(innerBindingKey)

    //compute Count Value Online
    var totalC = 0l

    while (partialLF.hasNext) {
      partialLF.next()
      var C = 1l;
      var i = 0
      while (i < eagerTableSize) {
        C *= eagerTables(i).getCount()
        i += 1
      }
      totalC += C
    }

    setCache(totalC)

    totalC
  }
}

class BindingAssociatedOutputTable extends BindingAssociatedCountTable {

  //binding conversion related variable, initialization required
  var outerBinding: Array[DataType] = _
  var innerBinding: Array[DataType] = _
  var innerBindingKey: mutable.WrappedArray[DataType] = _
  var outerBindingToInnerBindingArray: Array[AttributeID] = _

  //hashTable that records count per inner binding

  //java.util.HashMap
  val countTable: java.util.HashMap[mutable.WrappedArray[DataType], DataType] =
    new java.util.HashMap[mutable.WrappedArray[DataType], DataType]()

  //fastutil.HashMap
//  val countTable: Object2LongOpenHashMap[mutable.WrappedArray[DataType]] =
//    new Object2LongOpenHashMap[mutable.WrappedArray[DataType]](16, .5f)

  lazy val innerBindingSize = innerBindingKey.size

  def init(outerBinding: Array[DataType],
           outerBindingSchema: Array[AttributeID],
           innerBindingSchema: Array[AttributeID]): Unit = {

    innerBinding = new Array[DataType](innerBindingSchema.size)

    innerBindingKey = mutable.WrappedArray
      .make[DataType](innerBinding)
    this.outerBinding = outerBinding

    outerBindingToInnerBindingArray = innerBindingSchema
      .map(attrId => outerBindingSchema.indexOf(attrId))
  }

  override def getCount(): DataType = {
    var i = 0
    while (i < innerBindingSize) {
      innerBinding(i) = outerBinding(outerBindingToInnerBindingArray(i))
      i += 1
    }

    countTable.getOrDefault(innerBindingKey, 0)
  }

  def increment(value: DataType) = {
    var i = 0
    while (i < innerBindingSize) {
      innerBinding(i) = outerBinding(outerBindingToInnerBindingArray(i))
      i += 1
    }

    val oldValue = countTable.getOrDefault(innerBindingKey, 0)
    val newValue = oldValue + value

    if (oldValue == 0) {
      val key = mutable.WrappedArray
        .make[DataType](new Array[DataType](innerBindingSize))
      var i = 0
      while (i < innerBindingSize) {
        key(i) = innerBinding(i)
        i += 1
      }
      countTable.put(key, newValue)
    } else {
//      countTable.addTo()
      countTable.replace(innerBindingKey, newValue)
    }
  }

  def toArrays(): Array[Array[DataType]] = {
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

class ArrayLongLRUCache(cacheSize: Int = 1000000)
    extends LRUCache[mutable.WrappedArray[DataType], DataType](cacheSize) {
  def apply(key: mutable.WrappedArray[DataType]): DataType =
    get(key)
  def update(key: mutable.WrappedArray[DataType], value: DataType): Unit =
    put(key, value)
}
