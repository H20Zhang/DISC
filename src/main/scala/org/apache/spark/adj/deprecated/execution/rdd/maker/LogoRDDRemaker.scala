package org.apache.spark.adj.deprecated.execution.rdd.maker

import org.apache.spark.adj.deprecated.execution.rdd.RowLogoBlock
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

abstract class LogoRDDRemaker[A: ClassTag, B: ClassTag](rdd: RDD[RowLogoBlock[(A, B)]]) {

}

class simpleLogoRDDRemaker[A: ClassTag](rdd: RDD[RowLogoBlock[(List[Int], A)]]) extends LogoRDDRemaker(rdd) {

}
