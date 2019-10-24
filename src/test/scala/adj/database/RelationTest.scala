package adj.database

import org.apache.spark.adj.database.{Relation, RelationSchema}
import org.apache.spark.adj.execution.misc.DataLoader
import org.apache.spark.adj.utils.misc.SparkSingle
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class RelationTest extends FunSuite with BeforeAndAfterAll {

  val dataAdress = "./examples/wikiV.txt"
  var relations = Seq[Relation]()

  test("loading") {
    val name2 = "R2"
    val attrs2 = Seq("b", "c")
    val loader = new DataLoader()
    val relation2 =
      Relation(RelationSchema(name2, attrs2), loader.csv(dataAdress))

    relations = Seq(relation2, relation2)
  }

  override protected def afterAll(): Unit = {
    SparkSingle.close()
  }
}
