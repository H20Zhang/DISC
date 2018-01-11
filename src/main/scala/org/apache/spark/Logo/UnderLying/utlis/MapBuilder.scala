package org.apache.spark.Logo.UnderLying.utlis

object MapBuilder {

  def fromListToMap[A](data:Seq[Seq[A]], keys:Set[Int]) ={

        data.groupBy(f => ListSelector.selectElements(f,keys))
            .map(f => (f._1,f._2.map(t => ListSelector.notSelectElements(t,keys))))
  }

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
