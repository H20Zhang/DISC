package Example

import org.apache.spark.Logo.Plan.FilteringCondition
import org.apache.spark.Logo.UnderLying.utlis.{EdgeLoader, EdgePatternLoader}
import org.scalatest.FunSuite

import scala.util.Random

//This example might run a bit slow, but don't worry the most of time it takes is in preprocessing of edge (remove duplicate edges, ...)
//In cluster, preprocessing only takes only a matter of seconds.
//This preprocessing is an preprocessing that is required for any framework, which transform graph into undirected graph.
class BasicExample extends FunSuite{

  val data = "./email-Eu-core.txt"

  //loading the edge into the systems.
  lazy val rawEdge = {
    new EdgeLoader(data) rawEdgeRDD
  }

  //partitioning the edge using hash function H:R=>h or each node.
  // hNumber._1 which specify the h of left node of edge,
  // hNumber._2 specify the h of right node of edge.
  // for simplicity we write edgeh1_h2
  def getEdge(hNumber: (Int, Int)) = {
    //partitioning the loaded edge using given number of partitions.
    new EdgePatternLoader(rawEdge, Seq(hNumber._1, hNumber._2)) edgeLogoRDDReference
  }

  //GJ Join Example
  lazy val triangleNew = {
    //get a partitioned edge4_4, h of leftNode is 4, h of rightNode is 4.
    val h1 = 4
    val h2 = 4

    //p is an pattern instance, pattern instance stores the id of each node of pattern.
    //p(0) works like p.nodeArray[0] which retrieve node0.
    //push downed symmetry breaking condition.
    val filteredEdge = getEdge(h1,h2).filter(p => p(0) < p(1),true)

    //for better reference, we gives filteredEdge three names
    val edge1 = filteredEdge
    val edge2 = filteredEdge
    val edge3 = filteredEdge

    //using GJ join to assemble the triangle
    //edge1 mapping node0 to node0 of triangle, node1 to node1 of triangle, which is ommit for simplicity of code.
    //edge2.to(1,2) means mapping node0 of edge2 to node1 of triangle, mapping node1 to 2 th node of triangle.
    //edge3.to(0,2) means mapping node0 of edge2 to node0 of triangle, mapping node1 to 1 th node of triangle.
    //nodes of edges which is edge2 and edge3, coincide on 2 is intersected like GJ join.
    //overall effect, node0 of edge2 is attached to node 1 of edge1, node0 of edge 3 is attached to node 0 of edge2.
    //node1 of edge2 and node1 of edge3 is intersected to form node2 of triangle.
    val triangle =  edge1.build(edge2.to(1,2),edge3.to(0,2))
    triangle
  }

  //Binary Join Example
  lazy val chordalSquareNew = {
    val h1 = 4
    val h2 = 4

    //in this example, we generate two kinds of edge, edge4_1, and edge4_4
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    //push downed symetry breaking condition
    val leftEdge = edge4_4.filter(p => p(0) < p(1), true)

    //here, the h number of each node of triangle is different, for triangle
    // node0.h = 4
    // node1.h = 4
    // node2.h = 1
    // must be carefule that only nodes with same number of h can be joined.
    val triangle = leftEdge.build(edge4_1.to(1,2), edge4_1.to(0,2))
    val indexTriangle = triangle

    //here we join a triangle with another triangle.
    //which is equals to the sql
    // select triangle.node0 as chordalSquare.node0, triangle.node1 as chordalSquare.node1, triangle.node2 as chordalSquare.node2, indexTriangle.node2 as chordalSquare.node3
    // from triangle, indexTriangle
    // where triangle.node0 = indexTriangle.node0, triangle.node1 = indexTriangle.node1, chordalSquare.node2 < chordalSquare.node3
    val chordalSquare = triangle
      //mapping node 0,1,2 of indexTriangle to 0,1,3 of chordalSquare
      .build(indexTriangle.to(0,1,3))
      //sysmetry breaking condition.
      .filter(p => p(2) < p(3))

    chordalSquare
  }

  test("BasicExample"){
    val triangle = triangleNew
    val chordalSquare = chordalSquareNew

    //counting the size of the triangle
    println("size of triangle is " + triangle.size())
    println("size of chordalSquare is" + chordalSquare.size())

    //generating a spark compatible version of triangle, triangleSpark works like any RDD in spark
    //triangle inside triangleSpark is not materialized, it will materialized one triangle at a time
    //in pipeline fashion to avoid overwhelmnig intermediate result.
    val triangleSpark = triangle.rdd()

    //this below two line form a hashmap that randomly assign a each node to a group from 0 to 9.
    val theList = getEdge(4,4).rdd().flatMap(f => Iterable(f.getValue(0),f.getValue(1))).distinct().collect()
    val theMap = theList.map(f => (f, Random.nextInt() % 10)).toMap

    //we trying to triangle per edge (we should not use symmetry breaking in triangle, using the existing triangle with symmetry breaking not hurt the demonstration purpose)
    //we only care about triangle induced by edges that two nodes all fall into same group. //filter(f => (theMap(f(0)) == theMap(f(1))))
    //we retrieve the node0 and node1 of triangle, which forms a edge. //map(f => ((f(0),f(1)),1))
    //we use reduceBy key to aggregate the triangle numbers for each edge, in this process all the optimzation spark has implemented such as "Combinator" is used automatically. //reduceByKey(_ + _)
    //we only take the first 10 edges for demonstration purpose (be notice, in ideal cases only the triangle of 10 edges should be calculated, however spark is not that able to do so, more optimization required)
    val trianglePerEdge10 = triangleSpark.filter(f => theMap(f(0)) == theMap(f(1))).map(f => ((f(0),f(1)),1)).reduceByKey(_ + _).take(10)

    //print out the result.
    trianglePerEdge10.foreach(println)
  }




}
