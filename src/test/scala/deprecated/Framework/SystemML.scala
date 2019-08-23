package deprecated.Framework

import hzhang.test.SystemML.SystemMLExp
import org.scalatest.FunSuite

class SystemML extends FunSuite{

  test("basic"){
    val systemML = new SystemMLExp
    systemML.testMatrix()
  }


}
