package deprecated.Plan

import org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.Decrapted.{HNumberDecider, LazyOptimizer, QTree, QTreeNode}
import org.scalatest.FunSuite

class QTreeTest extends FunSuite{



  test("Basic"){
    val edgeAB = QTreeNode.EdgeNode("A","B")
    val edgeBC = QTreeNode.EdgeNode("B","C")
    val wedgeABC = QTreeNode("Wedge",edgeAB,edgeBC)
    val triangle1 = QTreeNode("Triangle1",wedgeABC,QTreeNode.EdgeNode("A","C"))

    val edgeBD = QTreeNode.EdgeNode("B","D")
    val edgeCD = QTreeNode.EdgeNode("C","D")
    val wedgeBCD = QTreeNode("Wedge",edgeBC,edgeCD)
    val triangle2 = QTreeNode("Triangle2",wedgeBCD,QTreeNode.EdgeNode("B","C"))

    val chordalSquare = QTreeNode("ChordalSquare",triangle1,triangle2)

    val queryTree = QTree(chordalSquare)
    queryTree.printTree()
    val stringBuilder = new StringBuilder
    queryTree.leftDeepTraverse(f => stringBuilder.append("\n" + f.name))
    println(stringBuilder)


    val patternMap = Seq(("Computation",1),("Communication",50),("Machine",64),("Edge",1),("Wedge",300),("Triangle1",10),("Triangle2",10),("ChordalSquare",5000)).toMap
    val lazyMap = Seq(("Edge",true),("Wedge",false),("Triangle1",false),("Triangle2",true),("ChordalSquare",true)).toMap

    val costString = queryTree.costString(patternMap,lazyMap)

//    println(costString)
  }

  test("HnumberDecider"){
    val edgeAB = QTreeNode.EdgeNode("A","B")
    val edgeBC = QTreeNode.EdgeNode("B","C")
    val wedgeABC = QTreeNode("Wedge",edgeAB,edgeBC)
    val triangle1 = QTreeNode("Triangle1",wedgeABC,QTreeNode.EdgeNode("A","C"))

    val edgeBD = QTreeNode.EdgeNode("B","D")
    val edgeCD = QTreeNode.EdgeNode("C","D")
    val wedgeBCD = QTreeNode("Wedge",edgeBC,edgeCD)
    val triangle2 = QTreeNode("Triangle2",wedgeBCD,QTreeNode.EdgeNode("B","C"))

    val chordalSquare = QTreeNode("ChordalSquare",triangle1,triangle2)

    val queryTree = QTree(chordalSquare)
    queryTree.printTree()
    val stringBuilder = new StringBuilder
    queryTree.leftDeepTraverse(f => stringBuilder.append("\n" + f.name))
    println(stringBuilder)


    val patternMap = Seq(("Computation",1),("Communication",20),("Machine",64),("Edge",1),("Wedge",300),("Triangle1",10),("Triangle2",10),("ChordalSquare",5000)).toMap
    val lazyMap = Seq(("Edge",true),("Wedge",false),("Triangle1",false),("Triangle2",true),("ChordalSquare",true)).toMap

    val hNumberDecider = new HNumberDecider(patternMap,lazyMap,queryTree,0)
    println(hNumberDecider.costProgramming)

    hNumberDecider.invokeOctave()
  }

  test("lazyOptimizer"){
    val edgeAB = QTreeNode.EdgeNode("A","B")
    val edgeBC = QTreeNode.EdgeNode("B","C")
    val wedgeABC = QTreeNode("Wedge",edgeAB,edgeBC)
    val triangle1 = QTreeNode("Triangle1",wedgeABC,QTreeNode.EdgeNode("A","C"))

    val edgeBD = QTreeNode.EdgeNode("B","D")
    val edgeCD = QTreeNode.EdgeNode("C","D")
    val wedgeBCD = QTreeNode("Wedge",edgeBC,edgeCD)
    val triangle2 = QTreeNode("Triangle2",wedgeBCD,QTreeNode.EdgeNode("B","C"))

    val chordalSquare = QTreeNode("ChordalSquare",triangle1,triangle2)

    val queryTree = QTree(chordalSquare)
    queryTree.printTree()
    val stringBuilder = new StringBuilder
    queryTree.leftDeepTraverse(f => stringBuilder.append("\n" + f.name))
    println(stringBuilder)

    val patternMap = Seq(("Computation",1),("Communication",20),("Machine",64),("Edge",1),("Wedge",300),("Triangle1",10),("Triangle2",10),("ChordalSquare",5000)).toMap

    val lazyOptimizer = new LazyOptimizer(patternMap,queryTree)
    lazyOptimizer.optimize()


  }


}
