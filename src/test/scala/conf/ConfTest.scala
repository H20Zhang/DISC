package conf

import org.apache.spark.adj.conf.Conf
import org.scalatest.FunSuite

class ConfTest extends FunSuite{

  test("conf"){
    val conf = Conf.defaultConf()
    println(conf.taskNum())

  }
}
