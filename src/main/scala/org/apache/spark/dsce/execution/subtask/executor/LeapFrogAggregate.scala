package org.apache.spark.dsce.execution.subtask.executor

import org.apache.spark.adj.database.{Catalog, RelationSchema}
import org.apache.spark.adj.database.Catalog.DataType
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
import org.apache.spark.adj.execution.subtask.utils.{ArraySegment, LRUCache}
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
    val attrIdsOrder = info.attrIdsOrder
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
    val eagerTableSubInfos = info.eagerTableInfos
    eagerTableSubInfos.map { eagerTableSubInfo =>
      val eagerTable =
        new BindingAssociatedEagerTable(eagerTableSubInfo, schemaToTrieMap)
      eagerTable.init(lf.getBinding(), info.attrIdsOrder)
      eagerTable
    }.toArray
  }

  def constructLazyTables() = {
    val lazyTableSubInfos = info.lazyTableInfos
    lazyTableSubInfos.map { lazyTableSubInfo =>
      val lazyTable =
        new BindingAssociatedLazyTable(lazyTableSubInfo, schemaToTrieMap)
      lazyTable.init(lf.getBinding(), info.attrIdsOrder)
      lazyTable
    }.toArray
  }

  def constructOutputTable(): BindingAssociatedOutputTable = {
    val outputTable = new BindingAssociatedOutputTable()
    outputTable.init(
      lf.getBinding(),
      info.attrIdsOrder,
      info.coreAttrIds.toArray
    )

    outputTable
  }

  def aggregate(): Array[Array[DataType]] = {

    val lazyTableNum = lazyTables.size
    val eagerTableNum = eagerTables.size
    var i = 0

    while (lf.hasNext) {
      val t = lf.next()
      var C = 1

      i = 0
      while (i < lazyTableNum) {
        C = C * lazyTables(i).getCount()
        i += 1
      }

      i = 0
      while (i < eagerTableNum) {
        C = C * eagerTables(i).getCount()
        i += 1
      }

      outputTable.setCount(C + outputTable.getCount())
    }

    outputTable.toArrays()
  }

}

//TODO: refractor needed
abstract class BindingAssociatedCountTable {
//  def init(binding: Array[DataType])

//  var outerBinding: Array[DataType] = _
//  val inputBinding: Array[DataType] = initInnerBinding()
//  var outerBindingToInnerBindingArray: Array[DataType] = _
//
//  def initInnerBinding(): Array[DataType]
//
//  def initAssociation(binding: Array[DataType],
//                      outerAttrIdOrder: Array[DataType]): Unit = {
//    outerBindingToInnerBindingArray = info.attrIdsOrder
//      .map(attrId => outerAttrIdOrder.indexOf(attrId))
//  }

  def getCount(): DataType
}

class BindingAssociatedEagerTable(
  info: EagerTableSubInfo,
  schemaToTrieMap: Map[RelationSchema, TrieHCubeBlock]
) extends BindingAssociatedCountTable {

  var outerBinding: Array[DataType] = _
  val inputBinding: Array[DataType] =
    new Array[DataType](info.attrIdsOrder.size - 1)
  val inputArraySegement: ArraySegment = ArraySegment(inputBinding)
  val outputValues: ArraySegment = ArraySegment.newEmptyArraySegment()
  val tableSchema = info.schema
  val trie = schemaToTrieMap(tableSchema).content
  var outerBindingToInnerBindingArray: Array[DataType] = _

  def init(binding: Array[DataType],
           outerAttrIdOrder: Array[DataType]): Unit = {
    outerBindingToInnerBindingArray = info.attrIdsOrder
      .map(attrId => outerAttrIdOrder.indexOf(attrId))
  }

  override def getCount(): DataType = {

    //update the input binding
    var i = 0
    val inputBindingSize = inputBinding.size
    while (i < inputBindingSize) {
      inputBinding(i) = outerBinding(outerBindingToInnerBindingArray(i))
      i += 1
    }

    //query the trie to get the count value
    trie.nextLevel(inputArraySegement, outputValues)
    if (outputValues.size == 0) {
      Catalog.NotExists
    } else {
      outputValues(0)
    }
  }

}

class BindingAssociatedLazyTable(
  info: LazyTableSubInfo,
  schemaToTrieMap: Map[RelationSchema, TrieHCubeBlock]
) extends BindingAssociatedCountTable {

  //binding conversion related variable
  var outerBinding: Array[DataType] = _
  val inputBinding: Array[DataType] =
    new Array[DataType](info.attrIdsOrder.size - 1)
  val inputArraySegement: ArraySegment = ArraySegment(inputBinding)
  val outputValues: ArraySegment = ArraySegment.newEmptyArraySegment()
  var outerBindingToInnerBindingArray: Array[DataType] = _

  //misc variable
  var partialLeapFrogJoin: PartialLeapFrogJoin = _
  var eagerTables: Array[BindingAssociatedEagerTable] = _
  var lruCache: ArrayIntLRUCache = _
  var lruKey: mutable.WrappedArray[DataType] = mutable.WrappedArray
    .make[DataType](new Array[DataType](inputArraySegement.size))

  def init(binding: Array[DataType], outerAttrIdOrder: Array[DataType]) = {
    outerBindingToInnerBindingArray = info.attrIdsOrder
      .map(attrId => outerAttrIdOrder.indexOf(attrId))

    partialLeapFrogJoin = constructPatternIt()
    eagerTables = constructEagerTables()
    lruCache = new ArrayIntLRUCache()
  }

  def constructPatternIt(): PartialLeapFrogJoin = {
    //prepare trieConstructedLeapFrogJoinInfo
    val attrsIdOrder = info.attrIdsOrder
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
    val eagerCountTableSubInfos = info.eagerCountTableInfos
    eagerCountTableSubInfos.map { eagerCountTableSubInfo =>
      val eagerTable =
        new BindingAssociatedEagerTable(eagerCountTableSubInfo, schemaToTrieMap)
      eagerTable.init(partialLeapFrogJoin.getBinding(), info.attrIdsOrder)
      eagerTable
    }.toArray
  }

  protected def setCahce(content: DataType): Unit = {

    val keySize = inputBinding.size
    val lruKey =
      mutable.WrappedArray.make[DataType](new Array[DataType](keySize))

    var i = 0
    while (i < keySize) {
      lruKey(i) = inputBinding(i)
      i += 1
    }

    lruCache.put(lruKey, content)
  }

  protected def getCache(): DataType = {

    val keySize = lruKey.size
    var i = 0
    while (i < keySize) {
      lruKey(i) = inputBinding(i)
      i += 1
    }
    val content = lruCache.get(lruKey)
    content
  }

  override def getCount(): DataType = {
    //update the input binding
    var i = 0
    val inputBindingSize = inputBinding.size
    while (i < inputBindingSize) {
      inputBinding(i) = outerBinding(outerBindingToInnerBindingArray(i))
      i += 1
    }

    val cachedResult = getCache()

    if (cachedResult != null) {
      return cachedResult
    }

    partialLeapFrogJoin.setPrefix(inputArraySegement)

    //compute Count Value Online
    var totalC = 0
    while (partialLeapFrogJoin.hasNext) {
      val t = partialLeapFrogJoin.next()
      var C = 1
      var i = 0
      val eagerTableSize = eagerTables.size
      while (i < eagerTableSize) {
        val CurC = eagerTables(i).getCount()

        if (CurC == Catalog.NotExists) {
          C = 0
        } else {
          C *= CurC
        }

        i += 1
      }
      totalC += C
    }

    setCahce(totalC)

    totalC
  }
}

class BindingAssociatedOutputTable extends BindingAssociatedCountTable {

  //binding conversion related variable, initialization required
  var outerBinding: Array[DataType] = _
  var innerBinding: mutable.WrappedArray[DataType] = _
  var outerBindingToInnerBindingArray: Array[DataType] = _

  //hashTable that records count per inner binding
  val hashTable: mutable.HashMap[mutable.WrappedArray[DataType], DataType] =
    new mutable.HashMap()

  def init(outerBinding: Array[DataType],
           outerBindingSchema: Array[DataType],
           innerBindingSchema: Array[DataType]): Unit = {

    innerBinding = mutable.WrappedArray
      .make[DataType](new Array[DataType](innerBindingSchema.size))

    this.outerBinding = outerBinding

    outerBindingToInnerBindingArray = innerBindingSchema
      .map(attrId => outerBindingSchema.indexOf(attrId))
  }

  override def getCount(): DataType = {
    var i = 0
    val inputBindingSize = innerBinding.size
    while (i < inputBindingSize) {
      innerBinding(i) = outerBinding(outerBindingToInnerBindingArray(i))
      i += 1
    }

    hashTable.getOrElse(innerBinding, 0)
  }

  def setCount(value: DataType) = {

    val key = mutable.WrappedArray.make[Int](new Array[Int](innerBinding.size))
    var i = 0
    while (i < key.size) {
      key(i) = innerBinding(i)
      i += 1
    }

    hashTable(key) = value
  }

  def toArrays(): Array[Array[DataType]] = {
    hashTable.map(f => (f._1 :+ f._2).toArray).toArray
  }

}

class ArrayIntLRUCache(cacheSize: Int = 1000000)
    extends LRUCache[mutable.WrappedArray[DataType], DataType](cacheSize) {
  def apply(key: mutable.WrappedArray[DataType]): DataType =
    get(key)
  def update(key: mutable.WrappedArray[DataType], value: DataType): Unit =
    put(key, value)
}
