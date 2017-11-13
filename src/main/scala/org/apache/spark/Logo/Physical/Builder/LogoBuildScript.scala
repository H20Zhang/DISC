package org.apache.spark.Logo.Physical.Builder

import org.apache.spark.Logo.Physical.dataStructure.{LogoBlock, RowLogoBlock}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class LogoBuildScript(logoSteps:List[LogoBuildScriptStep]) {}

trait LogoBuildScriptStep


case class SnapPoint(lRDDID:Int, lRDDSlot:List[Int], rRDDID:Int, rRDDSlot:List[Int])
case class LogoBuildScriptOneOnOne[A:ClassTag, B:ClassTag, C:ClassTag](rdd1:RDD[LogoBlock[A]], rdd2:RDD[LogoBlock[B]], snapPoints:List[SnapPoint], handler: (LogoBlock[A],LogoBlock[B]) => LogoBlock[C]) extends LogoBuildScriptStep{}
case class LogoBuildScripMultiOnOne[A:ClassTag, B:ClassTag, C:ClassTag, D:ClassTag](rdd1:RDD[LogoBlock[A]], rdd2:RDD[LogoBlock[B]], rdd3:RDD[LogoBlock[C]], snapPoints:List[SnapPoint], handler: (LogoBlock[A],LogoBlock[B], LogoBlock[C]) => LogoBlock[D]) extends LogoBuildScriptStep{}