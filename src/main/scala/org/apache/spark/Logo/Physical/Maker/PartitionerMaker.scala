package org.apache.spark.Logo.Physical.Maker

import org.apache.spark.Logo.Physical.dataStructure.{CompositeParitioner, SlotPartitioner}


//Generate the CompositePartitioner
case class PartitionerMaker() {

  var _slotMapping:List[Int] = _
  var _slotSize:List[Int] = _

  def setSlotMapping(slotMapping:List[Int]) ={
    _slotMapping = slotMapping
    this
  }

  def setSlotSize(slotSize:List[Int]) = {
    _slotSize = slotSize
    this
  }

  def build(): CompositeParitioner ={

    require(_slotMapping.sorted.zip(_slotMapping).forall(f => f._1 == f._2), s"slotNum of partitioner must be in ascending order " +
      s"current order is ${_slotMapping}, expected order is ${_slotMapping.sorted}")

    val partitioners = _slotMapping.zipWithIndex.map{case (slotNum,index) => new SlotPartitioner(_slotSize(index),slotNum)}
    val compositeParitioner = new CompositeParitioner(partitioners)
    compositeParitioner
  }
}
