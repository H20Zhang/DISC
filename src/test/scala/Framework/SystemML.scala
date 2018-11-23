package Framework

import hzhang.framework.test.SystemML.SystemMLExp
import org.scalatest.FunSuite

class SystemML extends FunSuite{

  test("basic"){
    val systemML = new SystemMLExp
    systemML.testMatrix()
  }


}
