package utils

import org.apache.spark.adj.utils.misc.SparkSingle
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkFunSuite extends FunSuite with BeforeAndAfterAll {

  val sc = SparkSingle.getSparkContext()
  val spark = SparkSingle.getSparkSession()

  override protected def afterAll(): Unit = {
    SparkSingle.close()
  }
}
