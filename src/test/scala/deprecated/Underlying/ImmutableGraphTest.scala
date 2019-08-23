package deprecated.Underlying

import org.apache.spark.adj.utlis.ImmutableGraph
import org.scalatest.FunSuite

class ImmutableGraphTest extends FunSuite{




  test("graph"){
    val graph1 = ImmutableGraph(Seq((1,2),(2,3),(3,4)))

    assert(graph1.isConnected() == true)
    assert(graph1.containsCycle() == true)
//    println(graph1)

    val graph2 = graph1.nodeInducedSubgraph(Seq(1,2,3))
    val graph3 = graph1.nodeInducedSubgraph(Seq(1,4))

    println(graph3)
    println(graph3.isConnected())
//    println(graph2)


//    println(graph1.addEdges(Seq((1,3))))
//    println(graph2)
  }

  test("utilityTest"){
    val graph1 = ImmutableGraph(Seq((1,2)))

    assert(graph1.isConnected() == true)
    assert(graph1.containsCycle() == false)

    val graph2 = ImmutableGraph(Seq((1,2),(2,3),(1,3)))
//    graph2.mergeNode(1,3)
    println(graph2.mergeNode(1,3))


  }
}
