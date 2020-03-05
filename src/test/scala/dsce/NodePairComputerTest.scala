package dsce

import adj.SparkFunSuite
import org.apache.spark.dsce.testing.tool.NodePairComputer

class NodePairComputerTest extends SparkFunSuite {

  test("count") {
    val dataset = "./examples/wikiV.txt"
    val splitNum = "4"
    NodePairComputer.main(Array(dataset, splitNum))
  }

}
