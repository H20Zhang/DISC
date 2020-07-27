package disc.integration

import disc.SparkFunSuite
import org.apache.spark.disc.optimization.rule_based.aggregate.CountTableCache
import org.apache.spark.disc.testing.{ExpData, ExpQuery}
import org.apache.spark.disc.util.misc.{Conf, QueryType}
import org.apache.spark.disc.{SubgraphCounting}

class DISCMainTest extends SparkFunSuite {

//  ("facebook", "facebook.txt"),
//  ("reactcome", "reactcome.txt"),
//  ("as-caida", "as-caida.txt"),
//  ("to", "topology.txt"),

//  val dataset = "reactcome"
  val dataset = "eu"

  def getPhyiscalPlan(dataset: String, query: String) = {
    val data = ExpData.getDataAddress(dataset)
    val dmlString = new ExpQuery(data) getQuery (query)
    SubgraphCounting.optimizedPhyiscalPlan(dmlString)
  }

  def getLogicalPlan(dataset: String, query: String) = {
    val data = ExpData.getDataAddress(dataset)
    val dmlString = new ExpQuery(data) getQuery (query)
    SubgraphCounting.optimizedLogicalPlan(dmlString)
  }

  test("expEntry") {
    val data = ExpData.getDataAddress(dataset)
    val executeMode = "Count"
    //    val executeMode = "ShowPlan"
    val queryType = "HOM"

//    val platform = "Single"
    val platform = "Parallel"

    val queries = Seq("t50")

    queries.foreach { query =>
//      CountTableCache.reset()
      val command1 =
        s"-q $query -d ${data} -e $executeMode -u ${queryType} -c A -p $platform"

      SubgraphCounting.main(command1.split("\\s"))
    }
  }

  //check if whole pipeline can be compiled
  test("triangle") {
    val query = "triangle"
    val plan = getPhyiscalPlan(dataset, query)
    println(s"PhyiscalPlan:\n${plan.prettyString()}")

    val output = plan.count()
    println(s"Output Size:${output}")
  }

  test("3-node") {
    //check if whole pipeline can basically work

    val queries = Seq("wedge", "triangle")
    val discConf = Conf.defaultConf()
    discConf.queryType = QueryType.InducedISO

    queries.foreach { query =>
      val plan = getPhyiscalPlan(dataset, query)
      //      println(s"plan:\n${plan.prettyString()}")
      val outString = s"""
                           |----------------$query-------------------
                           |plan:\n${plan.prettyString()}
                           |patternSize:${plan.count()}
                           |""".stripMargin
      print(outString)
    }
  }

  test("4-node") {
    //check if whole pipeline can basically work

    val queries = Seq(
      "threePath",
      "threeStar",
      "triangleEdge",
      "square",
      "chordalSquare",
      "fourClique"
    )
    val discConf = Conf.defaultConf()
    discConf.queryType = QueryType.InducedISO

    queries.foreach { query =>
      val plan = getPhyiscalPlan(dataset, query)
      //      println(s"plan:\n${plan.prettyString()}")
      val outString = s"""
                         |----------------$query-------------------
                         |plan:\n${plan.prettyString()}
                         |""".stripMargin
      print(outString)
    }
  }

  //check if whole pipeline can basically work
  test("5-node") {
    //    val queries = Seq("wedge", "triangle")
    val discConf = Conf.defaultConf()
    discConf.queryType = QueryType.ISO

    val queries = Seq("house", "threeTriangle", "solarSquare", "near5Clique")
//    val queries = Seq("solarSquare")
//    val queries = Seq("house")
    queries.foreach { query =>
      CountTableCache.reset()
      val plan = getPhyiscalPlan(dataset, query)
//      println(s"plan:\n${plan.prettyString()}")
      val outString = s"""
                         |----------------$query-------------------
                         |plan:\n${plan.prettyString()}
                         |patternSize:${plan.count()}
                         |""".stripMargin
      print(outString)
    }
  }

  //check if whole pipeline can basically work
  test("6-node") {
//    val queries = Seq("wedge", "triangle")
    val discConf = Conf.defaultConf()
    discConf.queryType = QueryType.ISO

    val queries = Seq(
      "quadTriangle",
      "triangleCore",
      "twinCSquare",
      "twinClique4",
      "starofDavidPlus"
    )
    queries.foreach { query =>
      CountTableCache.reset()
      val plan = getPhyiscalPlan(dataset, query)
      println(s"plan:\n${plan.prettyString()}")
//      val outString = s"""
//                         |----------------$query-------------------
//                         |plan:\n${plan.prettyString()}
//                         |patternSize:${plan.count()}
//                         |""".stripMargin
//      print(outString)
    }
  }

}
