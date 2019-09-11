package org.apache.spark.adj.execution.hcube

import org.apache.spark.adj.execution.subtask.SubTask
import org.apache.spark.rdd.RDD

trait HCube {
  def genHCubeRDD(): RDD[SubTask]
}
