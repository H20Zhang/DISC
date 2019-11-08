package org.apache.spark.dsce.deprecated.optimization.subgraph.visualization

import java.io.{File, PrintWriter}
import java.util.UUID

import org.apache.spark.dsce.TestAble

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait VisualizeAble {
  def toStringWithIndent(level: Int): String
  def addIndent(level: Int, str: String) = {
    str
      .split("\n")
      .map(f => s"\n${Seq.fill(level)("\t").foldLeft("")(_ + _)}$f")
      .reduce(_ + _)
      .drop(1 + level)
  }
  def toViz(): String
  lazy val objID: String = UniID.uniqueID()

  def display() = {
    val vizBuilder = GraphVizBuilder.newInstance()
    vizBuilder.addGraph(this)
    val visualizer = new GraphVisualizer(vizBuilder)
    visualizer.display()
  }
}

object UniID {
  var id = 0
  def uniqueID() = {
    id = id + 1
    id.toString
  }
}

class GraphVisualizer(builder: GraphVizBuilder) {

  def exportDot(address: String): Unit = {
    val writer = new PrintWriter(new File(s"${address}.gv"))
    writer.write(builder.toString)
  }

  def exportEps(address: String): Unit = {

    import sys.process._

    val gvAddress = s"${address}.gv"
    val gvFile = new File(s"${gvAddress}")
    val epsFile = new File(s"${address}.eps")

    val writer = new PrintWriter(gvFile)
    writer.write(builder.toString)
    writer.close()

//    execute the dot command to visualize the graph
    (s"dot -Teps ${gvAddress}" #> epsFile).!

    gvFile.deleteOnExit()
  }

  def display(): Unit = {
    import sys.process._

    val id = UUID.randomUUID().toString
    val tmpAddress = "./viz/tmp" + id

    exportEps(tmpAddress)

    s"""open ${tmpAddress}.eps""".!

    val tmpFile = new File(s"${tmpAddress}.eps")
    tmpFile.deleteOnExit()
  }
}

class GraphVizBuilder extends TestAble {

  private val _subgraphs: mutable.Buffer[String] = ArrayBuffer()
  private var _caption: String = _

  def addSubgraph() = ???
  def addEdgeBetweenSubgraph() = ???

  def addGraph(vizObj: VisualizeAble) = {
    _subgraphs += vizObj.toViz()
    this
  }

  def addCaption(caption: String) = {
    _caption = caption
    this
  }

  override def toString: String = {
    s"""
       |digraph hypertree {
       |compound=true;
       |  graph [pad="2", nodesep="0.7", ranksep="0.1", layout=fdp];
       |
       |
       |
       |  ${_subgraphs.map(str => s"${str} \n \n").reduce(_ + _)}
       |
       |
       |}
     """.stripMargin
  }

  //  TODO: test graphviz-java
  override def test(): Unit = {}
}

class SubgraphVizBuilder {

  def setCaption() = ???
  def addCaption() = ???

  def addNode() = ???
  def addEdge() = ???
}

object SubgraphVizBuilder {
  def newInstance() = {
    new SubgraphVizBuilder
  }
}

object GraphVizBuilder {
  def newInstance() = {
    new GraphVizBuilder()
  }
}
