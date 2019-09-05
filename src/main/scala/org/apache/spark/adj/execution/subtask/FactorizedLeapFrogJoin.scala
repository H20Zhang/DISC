package org.apache.spark.adj.execution.subtask

import org.apache.spark.adj.database.Catalog.DataType

//TODO: test
class FactorizedLeapFrogJoin(task: FactorizedLeapFrogJoinSubTask)
    extends LeapFrogJoin(task) {

  val corePos = task.corePos
  var isCoreChange = true
  val cache = new Array[Array[DataType]](attrSize)
  //construct the unary iterator for the i-th attribute given prefix consisting of 0 to i-1th attribute
  //Noted: this class wouldn't produce empty unary iterator unless all prefix for 0 to i-1th attribute has been tested.
  override protected def fixIterators(idx: Int): Unit = {

    if (hasEnd != true) {
      //case: fix the iterator for first attribute, this will be fixed only once
      if (idx == 0) {
        val it = constructIthIterator(idx)

        if (it.hasNext) {
          unaryIterators(idx) = it
          return
        } else {
          hasEnd = true
          return
        }
      }

      //case: fix the iterator for the rest of the attributes

      val prevIdx = idx - 1
      val prevIterator = unaryIterators(prevIdx)
      while (prevIterator.hasNext) {
        binding(prevIdx) = prevIterator.next()

        var it: Iterator[DataType] = null
        if (idx <= corePos + 1) {
          it = constructIthIterator(idx)

          if (idx <= corePos) {
            isCoreChange = true
          }

        } else {
          if (isCoreChange) {
            cache(idx) = constructIthIterator(idx).toArray
          }
          it = ArraySegment(cache(idx)).toIterator
        }

        if (it.hasNext) {
          unaryIterators(idx) = it
          return
        }
      }

      if (prevIdx == 0) {
        hasEnd = true
        return
      } else {
        fixIterators(idx - 1)
        fixIterators(idx)
      }
    }

  }
}
