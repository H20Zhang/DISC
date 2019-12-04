package org.apache.spark.adj.execution.subtask.utils

import org.apache.spark.adj.database.Catalog.DataType

import scala.collection.mutable.ArrayBuffer

class Binding {

  val array: Array[DataType] = null
  var end: Int = 0

  def partialBinding(i: Int) = {
    end = 0
    this
  }

  def setPos(i: Int, value: DataType) = {
    array(i) = value
  }

  def getPos(i: Int) = {
    array(i)
  }
}

case class ArraySegment(var array: Array[DataType],
                        var begin: Int,
                        var end: Int,
                        var size: Int) {

  def apply(i: Int) = {
    array(begin + i)
  }

  def update(i: Int, value: DataType) = {
    array(begin + i) = value
  }

  def set(_array: Array[DataType], _begin: Int, _end: Int, _size: Int): Unit = {
    array = _array
    begin = _begin
    end = _end
    size = _size
  }

  def slice(newBegin: Int, newEnd: Int): ArraySegment = {
    assert((newEnd + begin) < end)

    begin = begin + newBegin
    end = begin + newEnd
    size = end - begin

    this
  }

  def adjust(newBegin: Int, newEnd: Int): ArraySegment = {
    begin = newBegin
    end = newEnd
    size = end - begin

    this
  }

  def toArray() = {
    if (begin == 0 && size == array.size) {
      array
    } else {
      val buffer = ArrayBuffer[Int]()
      var i = begin
      while (i < end) {
        buffer += array(i)
        i += 1
      }

      buffer.toArray
    }
  }

  def toIterator = new Iterator[DataType] {
    var pos = begin

    override def hasNext: Boolean = pos < end

    override def next(): DataType = {
      val curPos = pos
      pos += 1
      array(curPos)
    }

    override def size: DataType = {
      end - begin
    }
  }

  override def toString: String = {
    val stringBuilder = new StringBuilder()
    var i = begin
    while (i < end) {
      stringBuilder.append(s"${array(i)}, ")
      i += 1
    }

    stringBuilder.dropRight(2).toString()
  }

}

object ArraySegment {
  val emptyArraySegment = ArraySegment(Array.emptyIntArray, 0, 0, 0)

  def emptyArray() = emptyArraySegment
  def newEmptyArraySegment() = ArraySegment(Array.emptyIntArray, 0, 0, 0)
  def apply(array: Array[DataType]): ArraySegment = {
    ArraySegment(array, 0, array.size, array.size)
  }
}
