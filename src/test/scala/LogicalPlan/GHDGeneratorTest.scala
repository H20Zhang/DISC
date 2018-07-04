package LogicalPlan

import org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize.GHDGenerator
import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{GHDNode, GHDTree, Relation, RelationSchema}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class GHDGeneratorTest extends FunSuite{

  test("GHDNode"){

    val relationSchema = RelationSchema.getRelationSchema()
    relationSchema.addRelation(Relation("R1",Seq("A","B"),10))
    relationSchema.addRelation(Relation("R2",Seq("B","C"),10))
    relationSchema.addRelation(Relation("R3",Seq("A","C"),10))
    relationSchema.addRelation(Relation("R4",Seq("C","D"),10))

    //initilization
    val nodeV1 = GHDNode()
    assert(nodeV1.toString == "GHDNode id:0 relations:ArrayBuffer()")

    val nodeV2 = GHDNode(ArrayBuffer(0))
    assert(nodeV2.toString == "GHDNode id:1 relations:ArrayBuffer(Relation(R1,List(A, B),10))")

    val nodeV3 = GHDNode(ArrayBuffer(0,1,2))
    assert(nodeV3.toString == "GHDNode id:2 relations:ArrayBuffer(Relation(R1,List(A, B),10), Relation(R2,List(B, C),10), Relation(R3,List(A, C),10))")

    val nodeV4 = GHDNode(ArrayBuffer(2,3))

    //functionality test
    assert(nodeV3.isConnected())
    assert(nodeV3.intersect(nodeV2).toString() == "(ArrayBuffer(0, 1),ArrayBuffer(0))")
    assert(nodeV3.estimatedCardinality()._2 == 1.5)
    assert(nodeV3.contains(nodeV2) == true)
    assert(nodeV3.contains(nodeV4) == false)
    assert(nodeV4.contains(nodeV3) == false)
  }

  test("GHDTree"){

    RelationSchema.reset()
    val relationSchema = RelationSchema.getRelationSchema()
    relationSchema.addRelation(Relation("R1",Seq("A","B"),10))
    relationSchema.addRelation(Relation("R2",Seq("B","C"),10))
    relationSchema.addRelation(Relation("R3",Seq("A","C"),10))
    relationSchema.addRelation(Relation("R4",Seq("C","D"),10))

    val nodeV1 = GHDNode()
    val nodeV2 = GHDNode(ArrayBuffer(0))
    val nodeV3 = GHDNode(ArrayBuffer(1))
    val nodeV4 = GHDNode(ArrayBuffer(2))

    //initialization
    val treeV1 = GHDTree()
    assert(treeV1.toString == "GHDTree id:0")
    assert(treeV1.isValid())

    val trees = treeV1.addNode(nodeV2)
//    assert(trees.map(_.toString).reduce(_ + _) == "GHDTree id:1 nodes:(4,GHDNode id:4 relations:ArrayBuffer(Relation(R1,List(A, B),10))) ImmutableGraph: Graph")

    val treeV2 = trees(0)
    val treesV2 = treeV2.addNode(nodeV3)
//    println("V2:")
//    treesV2.foreach(println)
    assert(treesV2(0).isValid() == true)
    assert(treeV2.fhw() == (1.0,1.0))

    val treeV3 = treesV2(0)
    val treesV3 = treeV3.addNode(nodeV4)
//    println("V3:")
//    treesV3.foreach(println)
    assert(treesV3.size == 3)
    treesV3.map(_.isValid()).foreach(f => assert(f == false))
  }

  test("GHDGenerator"){


    val queries = Seq(
//      "trianglePlsuOneEdge",
       "house"
//       "threeTriangle"
//      "near5Clique"
    )
    val queryAnswers = Seq("(GHDTree id:97 nodes:(76,GHDNode id:76 relations:ArrayBuffer(Relation(R4,List(C, D),10)))(77,GHDNode id:77 relations:ArrayBuffer(Relation(R1,List(A, B),10), Relation(R2,List(B, C),10), Relation(R3,List(A, C),10))) graph:ImmutableGraph (77,ArrayBuffer(76)) (76,ArrayBuffer(77)) ,1.5)"
      ,"(GHDTree id:1645 nodes:(789,GHDNode id:789 relations:ArrayBuffer(Relation(R1,List(A, B),1000000), Relation(R5,List(E, A),1000000), Relation(R6,List(B, E),500000)))(790,GHDNode id:790 relations:ArrayBuffer(Relation(R2,List(B, C),1000000), Relation(R3,List(C, D),1000000), Relation(R4,List(D, E),1000000), Relation(R6,List(B, E),500000))) graph:ImmutableGraph (790,ArrayBuffer(789)) (789,ArrayBuffer(790)) ,2.0)"
      ,"(GHDTree id:10321 nodes:(3549,GHDNode id:3549 relations:ArrayBuffer(Relation(R1,List(A, B),10), Relation(R2,List(B, C),10), Relation(R6,List(A, C),10)))(3554,GHDNode id:3554 relations:ArrayBuffer(Relation(R3,List(C, D),10), Relation(R6,List(A, C),10), Relation(R7,List(A, D),10)))(3555,GHDNode id:3555 relations:ArrayBuffer(Relation(R4,List(D, E),10), Relation(R5,List(E, A),10), Relation(R7,List(A, D),10))) graph:ImmutableGraph (3549,ArrayBuffer(3554)) (3555,ArrayBuffer(3554)) (3554,ArrayBuffer(3549, 3555)) ,1.5)"
      ,"(GHDTree id:55938 nodes:(16145,GHDNode id:16145 relations:ArrayBuffer(Relation(R2,List(B, C),10), Relation(R3,List(C, D),10), Relation(R4,List(D, E),10), Relation(R6,List(B, E),10), Relation(R7,List(C, E),10), Relation(R8,List(B, D),10)))(16152,GHDNode id:16152 relations:ArrayBuffer(Relation(R1,List(A, B),10), Relation(R5,List(E, A),10), Relation(R6,List(B, E),10))) graph:ImmutableGraph (16152,ArrayBuffer(16145)) (16145,ArrayBuffer(16152)) ,1.998)")


    def initlize(query:String) = query match {
      case "trianglePlsuOneEdge" => {
        RelationSchema.reset()
        val relationSchema = RelationSchema.getRelationSchema()
            relationSchema.addRelation(Relation("R1",Seq("A","B"),10))
            relationSchema.addRelation(Relation("R2",Seq("B","C"),10))
            relationSchema.addRelation(Relation("R3",Seq("A","C"),10))
            relationSchema.addRelation(Relation("R4",Seq("C","D"),10))
      }
      case "house" => {
        RelationSchema.reset()
        val relationSchema = RelationSchema.getRelationSchema()
        //    house
                relationSchema.addRelation(Relation("R1",Seq("A","B"),10))
                relationSchema.addRelation(Relation("R2",Seq("B","C"),10))
                relationSchema.addRelation(Relation("R3",Seq("C","D"),10))
                relationSchema.addRelation(Relation("R4",Seq("D","E"),10))
                relationSchema.addRelation(Relation("R5",Seq("E","A"),10))
                relationSchema.addRelation(Relation("R6",Seq("B","E"),10))
      }
      case "threeTriangle" => {
        RelationSchema.reset()
        val relationSchema = RelationSchema.getRelationSchema()
        //    threeTriangle
        relationSchema.addRelation(Relation("R1",Seq("A","B"),10))
        relationSchema.addRelation(Relation("R2",Seq("B","C"),10))
        relationSchema.addRelation(Relation("R3",Seq("C","D"),10))
        relationSchema.addRelation(Relation("R4",Seq("D","E"),10))
        relationSchema.addRelation(Relation("R5",Seq("E","A"),10))
        relationSchema.addRelation(Relation("R6",Seq("A","C"),10))
        relationSchema.addRelation(Relation("R7",Seq("A","D"),10))
      }
      case "near5Clique" => {
        RelationSchema.reset()
        val relationSchema = RelationSchema.getRelationSchema()
        //near5Clique
                  relationSchema.addRelation(Relation("R1",Seq("A","B"),10))
                  relationSchema.addRelation(Relation("R2",Seq("B","C"),10))
                  relationSchema.addRelation(Relation("R3",Seq("C","D"),10))
                  relationSchema.addRelation(Relation("R4",Seq("D","E"),10))
                  relationSchema.addRelation(Relation("R5",Seq("E","A"),10))
                  relationSchema.addRelation(Relation("R6",Seq("B","E"),10))
                  relationSchema.addRelation(Relation("R7",Seq("C","E"),10))
              relationSchema.addRelation(Relation("R8",Seq("B","D"),10))
      }
    }

    queries.zip(queryAnswers).foreach{case (query,answer) =>
      initlize(query)

      val relationSchema = RelationSchema.getRelationSchema()
      val gHDGenerator = new GHDGenerator((0 until relationSchema.relations.size).to[ArrayBuffer])
//      val optimalGHD = gHDGenerator.fhwOptimalGHD()
//      println(optimalGHD._1.nodes.toSeq(1))
//      println(optimalGHD._1.nodes.toSeq(1)._2.estimatedCardinality())
      println(gHDGenerator.fhwOptimalGHD().toString())
    }

  }

}



