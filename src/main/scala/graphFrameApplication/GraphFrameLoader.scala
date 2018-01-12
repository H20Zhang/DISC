//package graphFrameApplication
//
//import org.apache.spark.SparkContext
//import org.apache.spark.graphx.{Graph, GraphLoader}
//import org.graphframes.GraphFrame
//
//object GraphFrameLoader {
//
//  def load(sc:SparkContext, address:String): GraphFrame ={
//    val graph = GraphLoader.edgeListFile(sc,address)
//    val graphFrameGraph = GraphFrame.fromGraphX(graph)
//    graphFrameGraph
//  }
//
//}
