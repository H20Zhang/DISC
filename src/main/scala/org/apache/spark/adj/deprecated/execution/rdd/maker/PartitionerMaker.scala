package org.apache.spark.adj.deprecated.execution.rdd.maker

import org.apache.spark.adj.deprecated.execution.rdd.KeyMapping
import org.apache.spark.adj.deprecated.utlis.{CompositeParitioner, SlotPartitioner}


//Generate the CompositePartitioner
case class PartitionerMaker() {


  var _slotSizeMapping: KeyMapping = _
  lazy val _slotMapping = _slotSizeMapping.getKeys()
  lazy val _slotSize = _slotSizeMapping.getValues()


  def setSlotSizeMapping(slotSizeMapping: KeyMapping) = {
    _slotSizeMapping = slotSizeMapping
    this
  }

  def build(): CompositeParitioner = {

    val partitioners = _slotMapping.zipWithIndex.map { case (slotNum, index) => new SlotPartitioner(_slotSize(index), slotNum) }
    val compositeParitioner = new CompositeParitioner(partitioners)
    compositeParitioner
  }
}
