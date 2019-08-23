package org.apache.spark.adj.leapfrog

import org.apache.spark.adj.database.Database.{AttributeID, DataType}
import org.apache.spark.adj.plan.SubJoin

class LeapFrog(subJoins: SubJoin) extends Iterator[ArraySegment]{

  private val contents = subJoins.blocks.map(_.content)
  private val schemas = subJoins.blocks.map(_.schema)
  private val numRelations = schemas.size
  private val attrOrders = subJoins.attrOrders
  private val attrSize = attrOrders.size
  private val bindings = ArraySegment(new Array[DataType](attrSize))
  private var hasEnd = false

  private val tries = new Array[Trie](numRelations)
  private val unaryIterators = new Array[LeapFrogUnaryIterator](attrSize)

  override def hasNext: Boolean = {
    val lastIdx = attrSize - 1
    val lastIterator = unaryIterators(lastIdx)
    while (!hasEnd){
      if (!lastIterator.hasNext){
        fixIterators(lastIdx)
      }
    }
    ! hasEnd
  }


  override def next(): ArraySegment = {
    val lastIdx = attrSize - 1
    var lastIterator = unaryIterators(lastIdx)
    bindings(lastIdx) = lastIterator.next()
    bindings
  }


  private def reorderRelations(idx:Int):Unit = {
    val content = contents(idx)
    val schema = schemas(idx)

    //The func that mapping idx-th value of each tuple to j-th pos, where j-th position is the reletive order of idx-th attribute determined via attribute order
    val tupleMappingFunc = attrOrders.filter(schema.containAttribute).map(schema.globalIDTolocalIdx).zipWithIndex.reverse.sortBy(_._1).map(_._2).toArray
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

  private def initIterators():Unit = {
    var i = 0
    while(i < attrSize){
      fixIterators(i)
      i += 1
    }
  }

  private def fixIterators(idx:Int):Unit = {

    // fix the iterator for first attribute, this will be fixed only once
    if (hasEnd != true){
      if (idx == 0){
        val it = constructIthIterator(idx)

        if (it.hasNext){
          unaryIterators(idx) = it
          //        bindings(idx) = unaryIterators(idx).next()
          return
        } else {
          hasEnd = true
          return
        }
      }


      //fix the iterator for the rest of the attributes
      val prevIdx = idx - 1

      println(s"unaryIterators(${prevIdx}).content:${unaryIterators(prevIdx).content}")

      while (unaryIterators(prevIdx).hasNext){
        bindings(prevIdx) = unaryIterators(prevIdx).next()
        println(s"bindings:${bindings.array.toSeq}")


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

  private def init() = {

    //init content of relations
    var i = 0
    while(i < numRelations){
      val content = contents(i)
      val schema = schemas(i)

      //reorder the content according to attribute order
      reorderRelations(i)

      //construct trie based on reordered content
      tries(i) = ArrayTrie(content, schema.arity)
      i += 1
    }

    initIterators()
  }

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
            (schema, schemaPos, attrPos)
        }

        prefixPosForEachRelation
    }
  }

  private def constructIthIterator(idx:Int) = {
    val prefixPosForEachRelation = relevantRelationForAttrMap(idx)
    val segmentArrays = new Array[ArraySegment](prefixPosForEachRelation.size)

    println(s"relevant relations:${prefixPosForEachRelation} of attribute:${attrOrders(idx)}")
//    println(s"relevant relations map:${relevantRelationForAttrMap.toSeq}")


    var i = 0
    val sizeOfSegmentArrays = segmentArrays.size
    while(i < sizeOfSegmentArrays){
      val curBinding = bindings.adjust(0, prefixPosForEachRelation(i)._3)
      val trie = tries(prefixPosForEachRelation(i)._2)
      segmentArrays(i) = trie.nextLevel(curBinding)
      i += 1
    }

    new LeapFrogUnaryIterator(segmentArrays)
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
    println(s"content of the first level iterator:${unaryIterators(0).content}")
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
        println(s"${i}-th iterators content:${unaryIterators(i).content}")
      }
      i += 1
    }
    println(seperatorLine)
    println()
    //test leapfrog iterator
  }


}

class LeapFrogUnaryIterator(arrays:Array[ArraySegment]) extends Iterator[DataType]{

  val content = Alg.leapfrogIntersection(arrays)
  var idx = -1
  var end = content.size

  override def hasNext: Boolean = {
    (idx+1) < end
  }

  override def next(): DataType = {
    idx += 1
    content(idx)
  }
}