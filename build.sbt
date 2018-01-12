
name := "Logo"

version := "1,0"

scalaVersion := "2.11.12"



resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided"
libraryDependencies +=  "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided"
libraryDependencies +=  "org.apache.spark" % "spark-graphx_2.11" % "2.2.0" % "provided"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.3" % "test"
//libraryDependencies += "graphframes" % "graphframes" % "0.5.0-spark2.1-s_2.11"
libraryDependencies += "com.koloboke" % "koloboke-compile" % "0.5.1" % "provided"
libraryDependencies += "com.koloboke" % "koloboke-impl-common-jdk8" % "1.0.0" % "runtime"

