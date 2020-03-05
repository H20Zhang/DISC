package org.apache.spark.dsce.util

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
class Counter {
  final private val counter = new AtomicLong(0)

  def getValue: Long = counter.get

  def increment(): Unit = {
    while ({
      true
    }) {
      val existingValue = getValue
      val newValue = existingValue + 1
      if (counter.compareAndSet(existingValue, newValue)) return
    }
  }

  def reset() = {
    counter.set(0)
  }
}

object Counter {
  lazy val defaultCounter = new Counter
  def getDefaultCounter() = {
    defaultCounter
  }

  def resetCounter() = {
    defaultCounter.reset()
  }
}
