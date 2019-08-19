package Novel

import hzhang.framework.test.Novel.Pregel
import org.scalatest.FunSuite

class PregelTest extends FunSuite{
  val data = "./wikiV.txt"
  val pregel = new Pregel(data)

  test("pregelPageRank"){
    pregel.pageRank(20)
  }
}
