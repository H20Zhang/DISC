package disc.integration

import java.io.File

import org.apache.spark.disc.optimization.cost_based.ghd_decomposition.graph.GraphUtil
import org.apache.spark.disc.util.misc.{Counter, QueryHelper}
import org.apache.spark.disc.util.querygen.UniqueQueryComputer
import org.scalatest.FunSuite

class UniqueQueryComputerTest extends FunSuite {

  /*
  #numNode      1 2 3 4   5   6     7
  #numPattern   1 1 2 6   21  112   853
  #numQuery(node orbit)     1 1 3 11  58  407   4306
  #numQuery(edge orbit) 0 1 2 10 57 486
  #numQuery(triangle orbit) 0 0 1 3 21 197 2752
  #numQuery(node pair orbit) 0 1 1 8 67 701 10047
   */
  val numNode = 4
  val queryComputer = new UniqueQueryComputer(numNode)

  test("genPattern") {
    val patterns = queryComputer.genValidPattern()
    val dmls = patterns.map { f =>
      val V = f.V
      val E = f.E
      val Attrs = Seq("A", "B", "C", "D", "E")
      val IdToAttrsMap =
        V.zip(Attrs).toMap
      val EdgesOfAttrs =
        E.map(f => (IdToAttrsMap(f._1), IdToAttrsMap(f._2)))
          .map { f =>
            if (f._1 > f._2) {
              f.swap
            } else {
              f
            }
          }
          .sorted
      EdgesOfAttrs.map(f => s"${f._1}-${f._2};").reduce(_ + _)
    }

    dmls.foreach { dml =>
      val schemas = QueryHelper.dmlToSchemas(dml)
      println(schemas)
    }
    println(s"numPattern:${patterns.size}")
  }

  test("genQuery") {
    val queries = queryComputer.genValidPattern()
    println(s"numQuery:${queries.size}")
    val base = "./query"
    val counter = new Counter
    queries.zip(Range(1, queries.size+1)).
      foreach{
        case (p, index) =>


          if (p.isTree()){
            println(s"q${index}-acyclic)")
          } else {
              println(s"q${index}-cyclic")
          }
      }


    queries.foreach{
      f =>
        counter.increment()
        val id = counter.getValue
        val vLine = f.V.map(f => s"v ${f} -1").mkString("\n")
        val eLine = f.E.map(f => s"e ${f._1} ${f._2} -1").mkString("\n")
        val queryString = s"${vLine}\n${eLine}"
        import java.io.PrintWriter
        new PrintWriter(s"${base}/${numNode}node/q${id}") {
          write(queryString); close
        }
    }

//    val dmls = queries.map { f =>
//      val V = f.V
//      val E = f.E
//      val C = f.C
//      val Attrs = Seq("A", "B", "C", "D", "E")
//      val IdToAttrsMap =
//        (Seq((C.head, "A")) ++ V.diff(C).zip(Attrs.diff(Seq("A")))).toMap
//      val EdgesOfAttrs =
//        E.map(f => (IdToAttrsMap(f._1), IdToAttrsMap(f._2)))
//          .map { f =>
//            if (f._1 > f._2) {
//              f.swap
//            } else {
//              f
//            }
//          }
//          .sorted
//      EdgesOfAttrs.map(f => s"${f._1}-${f._2};").reduce(_ + _)
//    }
//    dmls.zipWithIndex.foreach {
//      case (dml, idx) =>
////        val str = s"""private val t${idx + 1} = \"${dml}\" """
//        val caseCaluse = s"""case \"t${idx + 1}\" => t${idx + 1}"""
//        println(caseCaluse)
////        println(str)
//    }
//
//    println(Range(1, 59))
  }
}
