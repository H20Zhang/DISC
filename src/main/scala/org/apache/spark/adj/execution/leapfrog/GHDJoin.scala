package org.apache.spark.adj.execution.leapfrog

import org.apache.spark.adj.database.Catalog.DataType
import org.apache.spark.adj.plan.{GHDJoinSubTask, LeapFrogJoinSubTask}

//TODO: finish this
class GHDJoin(subJoins: GHDJoinSubTask) extends Iterator[Array[DataType]] {

  def preMaterialize() = ???

  def yannakakis() = ???

  def finalAssemble() = ???

  override def hasNext: Boolean = ???

  override def next(): Array[DataType] = ???
}
