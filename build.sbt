import scala.sys.process._

name := "ADJ"
version := "0.1.1"
scalaVersion := "2.11.12"

/*Dependency*/
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0" % "provided"
libraryDependencies += "com.joptimizer" % "joptimizer" % "5.0.0"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.3" % "test"
libraryDependencies += "net.sf.trove4j" % "trove4j" % "3.0.3"
libraryDependencies += "com.typesafe" % "config" % "1.3.4"
libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.3.0"
// https://mvnrepository.com/artifact/com.github.scopt/scopt
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"

watchSources += baseDirectory.value / "script/"

//assemblyOption in assembly := (assemblyOption in assembly).value.copy(appendContentHash = true)
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheOutput = false)
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheUnzip = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp.filter { f =>
    f.data.getName == "systemml-1.1.0.jar" ||
    f.data.getName == "SCPSolver.jar" ||
    f.data.getName == "LPSOLVESolverPack.jar" ||
    f.data.getName == "GLPKSolverPack.jar"
  }
}

test in assembly := {}

/*Custom tasks*/
lazy val upload = taskKey[Unit]("Upload the files")
upload := {
  "./script/upload.sh" !
}

lazy val assembleThenUpload = taskKey[Unit]("Upload the jar after assembly")
assembleThenUpload := {
  assembly.value
  "./script/upload.sh" !
}
