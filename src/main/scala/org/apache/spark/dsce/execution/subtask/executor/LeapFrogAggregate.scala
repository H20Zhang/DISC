package org.apache.spark.dsce.execution.subtask.executor

import org.apache.spark.adj.database.Catalog.DataType
import org.apache.spark.adj.execution.subtask.executor.LongSizeIterator
import org.apache.spark.dsce.execution.subtask.LeapFrogAggregateSubTask

class LeapFrogAggregate(aggTask: LeapFrogAggregateSubTask)
    extends LongSizeIterator[Array[DataType]] {
  override def longSize(): Long = ???

  override def hasNext: Boolean = ???

  override def next(): Array[DataType] = ???
}
