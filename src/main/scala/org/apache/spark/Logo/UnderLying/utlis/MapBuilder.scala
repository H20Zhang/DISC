package org.apache.spark.Logo.UnderLying.utlis

//import com.koloboke.collect.map.hash.HashObjObjMaps

import org.apache.spark.Logo.UnderLying.dataStructure.{KeyPatternInstance, OneKeyPatternInstance, TwoKeyPatternInstance, ValuePatternInstance}

import scala.collection.mutable
import scala.collection.mutable.AnyRefMap.AnyRefMapBuilder
import scala.collection.mutable.ArrayBuffer

object MapBuilder {

  def fromListToMap[A](data:Seq[Seq[A]], keys:Set[Int]) ={

        val res = data.groupBy(f => ListSelector.selectElements(f,keys))
            .map(f => (f._1,f._2.map(t => ListSelector.notSelectElements(t,keys))))
    res
  }

  def fromListToMapFast[A](data:Seq[Seq[A]], keys:Set[Int]) ={

    val hashmap = new mutable.HashMap[Seq[A],ArrayBuffer[Seq[A]]]()


    data.foreach{
            f =>
              val key = ListSelector.selectElements(f,keys)

              val value = ListSelector.notSelectElements(f,keys)
              if (hashmap.contains(key)){
                hashmap.get(key).get.append(value)
              }else{
                hashmap.put(key,new ArrayBuffer[Seq[A]]())
                hashmap.get(key).get.append(value)
              }
          }

          hashmap
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
