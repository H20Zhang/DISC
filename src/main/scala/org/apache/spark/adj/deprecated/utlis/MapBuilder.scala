package org.apache.spark.adj.deprecated.utlis

//import com.koloboke.collect.map.hash.HashObjObjMaps

import org.apache.spark.adj.deprecated.execution.rdd._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

object MapBuilder {

  def fromListToMap[A](data: Seq[Seq[A]], keys: Set[Int]) = {

    val res = data.groupBy(f => ListSelector.selectElements(f, keys))
      .map(f => (f._1, f._2.map(t => ListSelector.notSelectElements(t, keys))))
    res
  }

  def fromListToMapFast[A](data: Seq[Array[A]], keys: Set[Int]) = {

    val hashmap = new mutable.HashMap[Seq[A], ArrayBuffer[Seq[A]]]()


    data.foreach {
      f =>
        val key = ListSelector.selectElements(f, keys)

        val value = ListSelector.notSelectElements(f, keys)
        if (hashmap.contains(key)) {
          hashmap.get(key).get.append(value)
        } else {
          hashmap.put(key, new ArrayBuffer[Seq[A]]())
          hashmap.get(key).get.append(value)
        }
    }

    hashmap
  }

  def fromListToMapLongFast(data: Seq[Array[Int]], keySet: Set[Int], keys: Seq[Int]) = {

    //    val hashmap = new mutable.HashMap[Seq[Long],ArrayBuffer[Seq[Long]]]()

    val hashmap = new mutable.LongMap[ArrayBuffer[ValuePatternInstance]]()
    val valueSize = data(0).length - keys.size

    if (keys.size == 1) {
      data.foreach {
        f =>
          val key = f(keys(0)).toLong
          //            ListSelector.selectElements(f,keys)
          val value = ListSelector.notSelectElements(f, keySet)

          if (valueSize == 1) {
            if (hashmap.contains(key)) {
              hashmap.get(key).get.append(ValuePatternInstance(value(0)))
            } else {
              hashmap.put(key, new ArrayBuffer[ValuePatternInstance]())
              hashmap.get(key).get.append(ValuePatternInstance(value(0)))
            }
          } else if (valueSize == 2) {
            if (hashmap.contains(key)) {
              hashmap.get(key).get.append(ValuePatternInstance(value(0), value(1)))
            } else {
              hashmap.put(key, new ArrayBuffer[ValuePatternInstance]())
              hashmap.get(key).get.append(ValuePatternInstance(value(0), value(1)))
            }
          } else {
            if (hashmap.contains(key)) {
              hashmap.get(key).get.append(ValuePatternInstance(value))
            } else {
              hashmap.put(key, new ArrayBuffer[ValuePatternInstance]())
              hashmap.get(key).get.append(ValuePatternInstance(value))
            }
          }
      }
    } else if (keys.size == 2) {
      data.foreach {
        f =>
          val key1 = f(keys(0))
          val key2 = f(keys(1))
          val key = (key1.toLong << 32) | (key2 & 0xffffffffL)
          val value = ListSelector.notSelectElements(f, keySet)


          if (valueSize == 1) {
            if (hashmap.contains(key)) {
              hashmap.get(key).get.append(ValuePatternInstance(value(0)))
            } else {
              hashmap.put(key, new ArrayBuffer[ValuePatternInstance]())
              hashmap.get(key).get.append(ValuePatternInstance(value(0)))
            }
          } else if (valueSize == 2) {
            if (hashmap.contains(key)) {
              hashmap.get(key).get.append(ValuePatternInstance(value(0), value(1)))
            } else {
              hashmap.put(key, new ArrayBuffer[ValuePatternInstance]())
              hashmap.get(key).get.append(ValuePatternInstance(value(0), value(1)))
            }
          } else {
            if (hashmap.contains(key)) {
              hashmap.get(key).get.append(ValuePatternInstance(value))
            } else {
              hashmap.put(key, new ArrayBuffer[ValuePatternInstance]())
              hashmap.get(key).get.append(ValuePatternInstance(value))
            }

          }
      }
    }


    val x = 0
    if (valueSize == 1) {
      hashmap.mapValuesNow { f =>
        val array = f.map(_.getValue(0)).toArray
        Sorting.quickSort(array)
        array.map(f => ValuePatternInstance(Seq(f)))
        //      f.toArray
      }
    }
    else {
      hashmap.mapValuesNow(f => f.toArray)
    }
    //    hashmap
  }


  def fromListToMapLongFastCompact(data: Iterator[PatternInstance], keySet: Set[Int], keys: Seq[Int], values:Seq[Int] ,valueSize:Int, needSorting:Boolean) = {

    val hashmap = new mutable.LongMap[CompactListAppendBuilder]()

    if (keys.size == 1) {
      if (valueSize == 1){
        val valueIndex0 = values(0)
        val keyIndex0 = keys(0)
        data.foreach {
          f =>
            val key = f.getValue(keyIndex0).toLong

            val res = hashmap.getOrNull(key)
            if (res != null) {
              res.append(f.getValue(valueIndex0))
            } else {
              hashmap.put(key, new CompactListAppendBuilder(valueSize))
              hashmap.getOrNull(key).append(f.getValue(valueIndex0))
            }
        }
      } else if (valueSize == 2){

        val valueIndex0 = values(0)
        val valueIndex1 = values(1)
        val keyIndex0 = keys(0)
        data.foreach {
          f =>
            val key = f.getValue(keyIndex0).toLong

            val res = hashmap.getOrNull(key)
            if (res != null) {
              res.append(f.getValue(valueIndex0))
              res.append(f.getValue(valueIndex1))
            } else {
              hashmap.put(key, new CompactListAppendBuilder(valueSize))
              val list = hashmap.getOrNull(key)
              list.append(f.getValue(valueIndex0))
              list.append(f.getValue(valueIndex1))
            }
        }

      } else{
        data.foreach {
          f =>
            val key = f.getValue(keys(0)).toLong
            val value = ListSelector.notSelectElementsIntPattern(f, keySet)

            hashmap.put(key, new CompactListAppendBuilder(valueSize))
            val list = hashmap.getOrNull(key)
            var i = 0
            while (i < valueSize) {
              list.append(value(i))
              i += 1
            }
        }
      }
    } else if (keys.size == 2) {

      if (valueSize == 1){
        val valueIndex0 = values(0)
        val keyIndex0 = keys(0)
        val keyIndex1 = keys(1)
        data.foreach {
          f =>
            val key1 = f.getValue(keyIndex0)
            val key2 = f.getValue(keyIndex1)
            val key = (key1.toLong << 32) | (key2 & 0xffffffffL)

            val res = hashmap.getOrNull(key)
              if (res != null) {
                res.append(f.getValue(valueIndex0))
              } else {
                hashmap.put(key, new CompactListAppendBuilder(valueSize))
                hashmap.getOrNull(key).append(f.getValue(valueIndex0))
              }

        }
      } else if (valueSize == 2){

        val valueIndex0 = values(0)
        val valueIndex1 = values(1)
        val keyIndex0 = keys(0)
        val keyIndex1 = keys(1)
        data.foreach {
          f =>
            val key1 = f.getValue(keyIndex0)
            val key2 = f.getValue(keyIndex1)
            val key = (key1.toLong << 32) | (key2 & 0xffffffffL)

            val res = hashmap.getOrNull(key)
            if (res != null) {
              res.append(f.getValue(valueIndex0))
              res.append(f.getValue(valueIndex1))
            } else {
              hashmap.put(key, new CompactListAppendBuilder(valueSize))
              val list = hashmap.getOrNull(key)
              list.append(f.getValue(valueIndex0))
              list.append(f.getValue(valueIndex1))
            }
        }

      } else{
        data.foreach {
          f =>
            val key1 = f.getValue(keys(0))
            val key2 = f.getValue(keys(1))
            val key = (key1.toLong << 32) | (key2 & 0xffffffffL)
            val value = ListSelector.notSelectElementsIntPattern(f, keySet)

                hashmap.put(key, new CompactListAppendBuilder(valueSize))
                val list = hashmap.getOrNull(key)
                var i = 0
                while (i < valueSize) {
                  list.append(value(i))
                  i += 1
                }
            }
        }
      }

    var count = 0L

    if (valueSize == 1) {
      val res = hashmap.mapValuesNow { f =>
        val compactList = f.toCompactList()
        if (needSorting){
          Sorting.quickSort(compactList.asInstanceOf[CompactOnePatternList].rawData)
        }
//        count += compactList.getRaw().length
        compactList
      }
      res.repack()
//      println(s"MapBuilder:$count")
      res

    }

    else {
      val res = hashmap.mapValuesNow{f =>
        val compactList = f.toCompactList()
//        count += compactList.getRaw().length
        compactList
      }
      res.repack()
//      println(s"MapBuilder:$count")
      res
    }

  }

  //  def fromListToMapFast[A](data:Seq[Seq[A]],keys:Set[Int]) = {
  //    val hashmap = HashObjObjMaps.newMutableMap[Seq[A],ArrayBuffer[Seq[A]]](data.size)
  //
  //    data.foreach{
  //      f =>
  //        val key = ListSelector.selectElements(f,keys)
  //        val value = ListSelector.notSelectElements(f,keys)
  //        if (hashmap.containsKey(key)){
  //          hashmap.get(key).append(value)
  //        }else{
  //          hashmap.put(key,new ArrayBuffer[Seq[A]]())
  //        }
  //    }
  //
  //    hashmap
  //  }
  //
  //  def oneKeyfromListToMapFast(data:Seq[Seq[Int]], keys:Set[Int]) = {
  //    val hashmap = HashObjObjMaps.newMutableMap[KeyPatternInstance,ArrayBuffer[ValuePatternInstance]](data.size)
  //
  //    data.foreach{
  //      f =>
  //        val raw_key = ListSelector.selectElements(f,keys)
  //        val key = new OneKeyPatternInstance(raw_key(0))
  //        val raw_value = ListSelector.notSelectElements(f,keys)
  //        val value = new ValuePatternInstance(raw_value)
  //        if (hashmap.containsKey(key)){
  //          hashmap.get(key).append(value)
  //        }else{
  //          hashmap.put(key,new ArrayBuffer[ValuePatternInstance]())
  //        }
  //    }
  //
  //    hashmap
  //  }
  //
  //  def twoKeyfromListToMapFast(data:Seq[Seq[Int]], keys:Set[Int]) = {
  //    val hashmap = HashObjObjMaps.newMutableMap[KeyPatternInstance,ArrayBuffer[ValuePatternInstance]](data.size)
  //
  //    data.foreach{
  //      f =>
  //        val raw_key = ListSelector.selectElements(f,keys)
  //        val key = new TwoKeyPatternInstance(raw_key(0),raw_key(1))
  //        val raw_value = ListSelector.notSelectElements(f,keys)
  //        val value = new ValuePatternInstance(raw_value)
  //        if (hashmap.containsKey(key)){
  //          hashmap.get(key).append(value)
  //        }else{
  //          hashmap.put(key,new ArrayBuffer[ValuePatternInstance]())
  //        }
  //    }
  //
  //    hashmap
  //  }
  //
  //  def keyfromListToMapFast(data:Seq[Seq[Int]], keys:Set[Int]) = {
  //    val hashmap = HashObjObjMaps.newMutableMap[KeyPatternInstance,ArrayBuffer[ValuePatternInstance]](data.size)
  //
  //    data.foreach{
  //      f =>
  //        val raw_key = ListSelector.selectElements(f,keys)
  //        val key = new KeyPatternInstance(raw_key)
  //        val raw_value = ListSelector.notSelectElements(f,keys)
  //        val value = new ValuePatternInstance(raw_value)
  //        if (hashmap.containsKey(key)){
  //          hashmap.get(key).append(value)
  //        }else{
  //          hashmap.put(key,new ArrayBuffer[ValuePatternInstance]())
  //        }
  //    }
  //
  //    hashmap
  //  }

  //In case of two building, this function is not needed, because there is only one leaf.
  //  def buildKeyValueMap[A](data:Seq[Seq[A]], keys:Set[Int], values:Set[Int]) ={
  //    require(keys.intersect(values).size == 0)
  //
  //    //because after the key Map is build the values position will change, so we need to calculate the new value position.
  //    val maxCol = (keys ++ values).max
  //    val indexList = Range(0,maxCol+1)
  //    val keySet = keys.toSet
  //    val valueSet = values.toSet
  //    val newValues = indexList.filter(p => !keySet.contains(p)).zipWithIndex.filter(p => valueSet.contains(p._1)).map(_._2)
  //
  //    val theMap = fromListToMap(data,keys)
  //    theMap.map(f => (f._1,fromListToMap(f._2,newValues)))
  //  }

}
