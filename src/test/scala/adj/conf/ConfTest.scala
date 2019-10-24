package adj.conf

import org.apache.spark.adj.utils.misc.Conf
import org.scalatest.FunSuite

class ConfTest extends FunSuite {

  test("adj/conf") {
    val conf = Conf.defaultConf()
    println(conf.getTaskNum())

  }
}
