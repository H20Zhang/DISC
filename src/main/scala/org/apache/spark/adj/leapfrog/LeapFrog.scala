package org.apache.spark.adj.leapfrog

import org.apache.spark.adj.database.Catalog.{AttributeID, DataType}
import org.apache.spark.adj.leapfrog.Intersection.seek
import org.apache.spark.adj.plan.SubJoin

import scala.collection.mutable.ArrayBuffer

class LeapFrog(subJoins: SubJoin) extends Iterator[Array[DataType]]{

  private val contents = subJoins.blocks.map(_.content).map(f => f.map(g => g.clone()))
  private val schemas = subJoins.blocks.map(_.schema)
  private val numRelations = schemas.size
  private val attrOrders = subJoins.attrOrders
  private val attrSize = attrOrders.size
  private val binding = new Array[DataType](attrSize)
  private var hasEnd = false
  val tries = new Array[Trie](numRelations)
  private val unaryIterators = new Array[Iterator[DataType]](attrSize)

  //record
  // 1) for each attribute: the relevant relations
  // 2)   for each relation: the index of prefix in binding
  //
  //output format:
  // Attributes(Relevant Relations(Relation Schema, Index of Relation Schema, Index of prefix, An temp array for store partial biniding))
  lazy val relevantRelationForAttrMap = {
    Range(0, attrSize).toArray.map{
      idx =>
        val curAttr = attrOrders(idx)
        val relevantRelation = schemas.zipWithIndex.filter{
          case (schema, schemaPos) =>
            schema.containAttribute(curAttr)
        }

        val prefixPosForEachRelation = relevantRelation.map{
          case (schema, schemaPos) =>

            val relativeOrder = attrOrders.filter(schema.containAttribute)
            val attrPos = relativeOrder.indexOf(curAttr)
            val partialBindingPos = new Array[DataType](attrPos)
            val partialBinding = ArraySegment(new Array[DataType](attrPos))

            var i = 0
            while(i < attrPos){
              partialBindingPos(i) = attrOrders.indexOf(relativeOrder(i))
              i += 1
            }

            (schema, schemaPos, partialBindingPos, partialBinding)
        }

        prefixPosForEachRelation
    }
  }

  //init the leapfrog
  init()

  private def init() = {

    //init content of relations
    var i = 0
    while(i < numRelations){


      //reorder the content according to attribute order
      reorderRelations(i)
//      println(s"schema:${schema.name}, content-${i}, content:${content.toSeq.map(_.toSeq)}")

      //construct trie based on reordered content
      tries(i) = ArrayTrie(contents(i), schemas(i).arity)
      i += 1
    }

    //init unary iterator for all attributes
    initIterators()
  }

  //reorder tuples of relation-idx according to attribute order
  private def reorderRelations(idx:Int):Unit = {
    val content = contents(idx)
    val schema = schemas(idx)

    //The func that mapping idx-th value of each tuple to j-th pos, where j-th position is the reletive order of idx-th attribute determined via attribute order
    val tupleMappingFunc = attrOrders.filter(schema.containAttribute).map(schema.attrIDs.indexOf(_))
//    .zipWithIndex.reverse.sortBy(_._1).map(_._2).toArray
    val contentArity = schema.arity
    val contentSize = content.size
    val tempArray = new Array[DataType](contentArity)

    var i = 0
    while(i < contentSize){
      val tuple = content(i)
      var j = 0

      while(j < contentArity){
        tempArray(j) = tuple(j)
        j += 1
      }

      j = 0
      while(j < contentArity){
        tuple(j) = tempArray(tupleMappingFunc(j))
        j += 1
      }

      i += 1
    }
  }

  //init unary iterator for all attributes
  private def initIterators():Unit = {

    //gradually init the unary iterator for 0, 1, ..., n-th attribute
    var i = 0
    while(i < attrSize){
      fixIterators(i)
      i += 1
    }
  }

  //construct the unary iterator for the i-th attribute given prefix consisting of 0 to i-1th attribute
  //Noted: this class wouldn't produce empty unary iterator unless all prefix for 0 to i-1th attribute has been tested.
  private def fixIterators(idx:Int):Unit = {

    if (hasEnd != true){
      //case: fix the iterator for first attribute, this will be fixed only once
      if (idx == 0){
        val it = constructIthIterator(idx)

        if (it.hasNext){
          unaryIterators(idx) = it
          return
        } else {
          hasEnd = true
          return
        }
      }

      //case: fix the iterator for the rest of the attributes
      val prevIdx = idx - 1
      while (unaryIterators(prevIdx).hasNext){
        binding(prevIdx) = unaryIterators(prevIdx).next()
        val it = constructIthIterator(idx)

        if (it.hasNext){
          unaryIterators(idx) = it
          return
        }
      }

      if (prevIdx == 0){
        hasEnd = true
      } else{
        fixIterators(idx-1)
        fixIterators(idx)
      }
    }

  }

  //construct the unary iterator for the i-th attribute given prefix consisting of 0 to i-1th attribute
  //Noted: this class could return empty unary iterator
  private def constructIthIterator(idx:Int) = {
    val prefixPosForEachRelation = relevantRelationForAttrMap(idx)
    val segmentArrays = new Array[ArraySegment](prefixPosForEachRelation.size)

    var i = 0
    val sizeOfSegmentArrays = segmentArrays.size
    while(i < sizeOfSegmentArrays){

      val partialBindingPos = prefixPosForEachRelation(i)._3
      val curBinding = prefixPosForEachRelation(i)._4
      var j = 0
      while (j < partialBindingPos.size){
        curBinding(j) = binding(partialBindingPos(j))
        j += 1
      }

      val trie = tries(prefixPosForEachRelation(i)._2)
      segmentArrays(i) = trie.nextLevel(curBinding)
      i += 1
    }

    //leapfrogIt still need debug
//    IntersectionIterator.leapfrogIt(segmentArrays)
    new IntersectedListIterator(segmentArrays)
  }

  val lastIdx = attrSize - 1
  override def hasNext: Boolean = {

    //check if last iterator hasNext, if not, trying to produce new last iterator
    val lastIterator = unaryIterators(lastIdx)
    if (!hasEnd){
      if (!lastIterator.hasNext){
        fixIterators(lastIdx)
      }
    }
    ! hasEnd
  }

  override def next():Array[DataType] = {
    val lastIterator = unaryIterators(lastIdx)
    binding(lastIdx) = lastIterator.next()
    binding
  }


  def debugInternal() = {

    val seperatorLine = s"-----------------------------------"
    //test dataset

    println(s"Testing Dataset")
    println(seperatorLine)
    var idx = 0
    while(idx < numRelations) {
      val schema = schemas(idx)
      val content = contents(idx)
      println(s"Relation:${schema.name}, Attrs:${schema.attrs}, AttrIDs:${schema.attrIDs}, AttrIDOrder:${attrOrders.toSeq}")
      println(s"Original Content:${content.toSeq.map(_.toSeq)}")
      idx += 1
    }
    println(seperatorLine)
    println()

    //test reorderRelationContent
    println(s"Testing reorder relation content")
    println(seperatorLine)

    idx = 0
    while(idx < numRelations) {
      val schema = schemas(idx)
      val content = contents(idx)
      reorderRelations(idx)
      println(s"Relation:${schema.name}, Attrs:${schema.attrs}, AttrIDs:${schema.attrIDs}, AttrIDOrder:${attrOrders.toSeq}")
      println(s"Reordered Content:${content.toSeq.map(_.toSeq)}")
      idx += 1
    }
    println(seperatorLine)
    println()

    //test construct iterator
    println(s"Testing construct iterator")
    println(seperatorLine)
    var i = 0
    while(i < numRelations){
      val content = contents(i)
      val schema = schemas(i)

      //prepare trie
      tries(i) = ArrayTrie(content, schema.arity)
      println(s"trie of Relation:${schema.name}")
      println(s"${tries(i).toRelation().toSeq.map(_.toSeq)}")

      i += 1
    }
//    val iterator1 =
    unaryIterators(0) = constructIthIterator(0)
//    println(s"content of the first level iterator:${unaryIterators(0).content}")
    println(seperatorLine)
    println()

    //test init iterator
    println(s"Testing init iterator")
    println(seperatorLine)

    //init iterators
    i = 1
    while(i < attrSize){
      fixIterators(i)
      i += 1
    }


    i = 0
    while(i < attrSize){
      if (unaryIterators(i) != null){
//        println(s"${i}-th iterators content:${unaryIterators(i).content}")
      }
      i += 1
    }
    println(seperatorLine)
    println()
    //test leapfrog iterator
  }
}




