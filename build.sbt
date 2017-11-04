
name := "Logo"

version := "0.1"

scalaVersion := "2.11.6"


resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"
libraryDependencies +=  "org.apache.spark" % "spark-core_2.11" % "2.2.0"
libraryDependencies +=  "org.apache.spark" % "spark-graphx_2.11" % "2.2.0"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.3" % "test"
libraryDependencies += "graphframes" % "graphframes" % "0.5.0-spark2.1-s_2.11"