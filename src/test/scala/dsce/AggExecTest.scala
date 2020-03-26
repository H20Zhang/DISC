package dsce

import adj.SparkFunSuite
import org.apache.spark.adj.utils.misc.Conf
import org.apache.spark.dsce.Query
import org.apache.spark.dsce.plan.{
  MultiplyAggregateExec,
  UnOptimizedCountAggregate,
  UnOptimizedSubgraphCount
}
import org.apache.spark.dsce.testing.{ExpData, ExpQuery}

class AggExecTest extends SparkFunSuite {

  val dataset = "eu"

  def getOptimizedPlan(dataset: String, query: String) = {
    val data = ExpData.getDataAddress(dataset)
    val dmlString = query
    val dml = new ExpQuery(data) getQuery (dmlString)
    val plan = Query.unOptimizedPlan(dml).asInstanceOf[UnOptimizedSubgraphCount]
    val unOptimizedCountAgg =
      UnOptimizedCountAggregate(plan.edge, plan.coreAttrIds)
    unOptimizedCountAgg.optimize().optimize()
  }

  //check if basic aggregation can work
  test("edge") {
    val optimizedMultiplyAgg = getOptimizedPlan(dataset, "edge")
    println(s"optimizedMultiplyAgg:\n${optimizedMultiplyAgg.prettyString()}")
    val multiplyAggExec =
      optimizedMultiplyAgg.phyiscalPlan().asInstanceOf[MultiplyAggregateExec]
    println(s"PhysicalMultiplyAgg:\n${multiplyAggExec.prettyString()}")

    multiplyAggExec.globalAggregate()
  }

  //check if basic aggregation can output correct value
  test("triangle") {
    val optimizedMultiplyAgg = getOptimizedPlan(dataset, "triangle")
    println(s"optimizedMultiplyAgg:\n${optimizedMultiplyAgg.prettyString()}")
    val multiplyAggExec =
      optimizedMultiplyAgg.phyiscalPlan().asInstanceOf[MultiplyAggregateExec]
    println(s"PhysicalMultiplyAgg:\n${multiplyAggExec.prettyString()}")

    multiplyAggExec.globalAggregate()
  }

  //check if eager aggregation can work
  test("wedge") {
    val optimizedMultiplyAgg = getOptimizedPlan(dataset, "wedge")
    println(s"optimizedMultiplyAgg:\n${optimizedMultiplyAgg.prettyString()}")
    val multiplyAggExec =
      optimizedMultiplyAgg.phyiscalPlan().asInstanceOf[MultiplyAggregateExec]
    println(s"PhysicalMultiplyAgg:\n${multiplyAggExec.prettyString()}")

    multiplyAggExec.globalAggregate()
  }

  //check if non-lazy aggregation can output correct value
  test("chordalSquare") {
    val optimizedMultiplyAgg = getOptimizedPlan(dataset, "chordalSquare")
    println(s"optimizedMultiplyAgg:\n${optimizedMultiplyAgg.prettyString()}")
    val multiplyAggExec =
      optimizedMultiplyAgg.phyiscalPlan().asInstanceOf[MultiplyAggregateExec]
    println(s"PhysicalMultiplyAgg:\n${multiplyAggExec.prettyString()}")

    multiplyAggExec.globalAggregate()
  }

  //check if non-lazy aggregation can output correct value
  test("threeTriangle") {
    val optimizedMultiplyAgg = getOptimizedPlan(dataset, "threeTriangle")
    println(s"optimizedMultiplyAgg:\n${optimizedMultiplyAgg.prettyString()}")
    val multiplyAggExec =
      optimizedMultiplyAgg.phyiscalPlan().asInstanceOf[MultiplyAggregateExec]
    println(s"PhysicalMultiplyAgg:\n${multiplyAggExec.prettyString()}")

    multiplyAggExec.globalAggregate()
  }

  //check if lazy aggregation can work
  test("square") {
    val optimizedMultiplyAgg = getOptimizedPlan(dataset, "square")
    println(s"optimizedMultiplyAgg:\n${optimizedMultiplyAgg.prettyString()}")
    val multiplyAggExec =
      optimizedMultiplyAgg.phyiscalPlan().asInstanceOf[MultiplyAggregateExec]
    println(s"PhysicalMultiplyAgg:\n${multiplyAggExec.prettyString()}")

    multiplyAggExec.globalAggregate()
  }

  //check if lazy aggregation and eager aggregation can work
  test("squareEdge") {
    val optimizedMultiplyAgg = getOptimizedPlan(dataset, "squareEdge")
    println(s"optimizedMultiplyAgg:\n${optimizedMultiplyAgg.prettyString()}")
    val multiplyAggExec =
      optimizedMultiplyAgg.phyiscalPlan().asInstanceOf[MultiplyAggregateExec]
    println(s"PhysicalMultiplyAgg:\n${multiplyAggExec.prettyString()}")

    multiplyAggExec.globalAggregate()
  }

  test("solarSquare") {
    val optimizedMultiplyAgg = getOptimizedPlan(dataset, "solarSquare")
    println(s"optimizedMultiplyAgg:\n${optimizedMultiplyAgg.prettyString()}")
    val multiplyAggExec =
      optimizedMultiplyAgg.phyiscalPlan().asInstanceOf[MultiplyAggregateExec]
    println(s"PhysicalMultiplyAgg:\n${multiplyAggExec.prettyString()}")

    multiplyAggExec.globalAggregate()
  }

  test("debug") {

//    Conf.defaultConf().setOneCoreLocalCluster()
    Conf.defaultConf().setLocalCluster()

    val optimizedMultiplyAgg = getOptimizedPlan(dataset, "t23")
//    val optimizedMultiplyAgg = getOptimizedPlan(dataset, "square")
//    val optimizedMultiplyAgg = getOptimizedPlan(dataset, "debug")
    println(s"optimizedMultiplyAgg:\n${optimizedMultiplyAgg.prettyString()}")
    val multiplyAggExec =
      optimizedMultiplyAgg.phyiscalPlan().asInstanceOf[MultiplyAggregateExec]
    println(s"PhysicalMultiplyAgg:\n${multiplyAggExec.prettyString()}")

    multiplyAggExec.globalAggregate()
  }
}
