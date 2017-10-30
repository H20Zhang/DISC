
name := "org/apache/spark/Logo"

version := "0.1"

scalaVersion := "2.11.6"


libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"
libraryDependencies +=  "org.apache.spark" % "spark-core_2.11" % "2.2.0"
libraryDependencies +=  "org.apache.spark" % "spark-graphx_2.11" % "2.2.0"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.3" % "test"