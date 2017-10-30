package org.apache.spark.Logo.dataStructure

import org.apache.spark.Partitioner

sealed trait LogoColType
case object KeyType extends LogoColType
case object NonKeyType extends LogoColType
case object AttributeType extends LogoColType

case class LogoSchema (partitioner:Partitioner, edges:List[(Int,Int)], KeyCol:List[Int], Cols:List[LogoColType]){}
