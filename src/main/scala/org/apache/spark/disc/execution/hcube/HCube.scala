package org.apache.spark.disc.execution.hcube

import org.apache.spark.disc.execution.subtask.SubTask
import org.apache.spark.rdd.RDD

trait HCube {
  def genHCubeRDD(): RDD[SubTask]
}
