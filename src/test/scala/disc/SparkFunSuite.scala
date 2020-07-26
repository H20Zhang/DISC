package disc

import org.apache.spark.disc.util.misc.SparkSingle
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkFunSuite extends FunSuite with BeforeAndAfterAll {

  val sc = SparkSingle.getSparkContext()
  val spark = SparkSingle.getSparkSession()

  override protected def afterAll(): Unit = {
    SparkSingle.close()
  }
}
