package conf

import org.apache.spark.adj.utils.misc.Conf
import org.scalatest.FunSuite

class ConfTest extends FunSuite {

  test("conf") {
    val conf = Conf.defaultConf()
    println(conf.taskNum())

  }
}
