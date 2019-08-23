package database

import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.utlis.SparkSingle
import org.scalatest.FunSuite

class RelationTest extends FunSuite{

  test("basic operation"){
    val dataAdress = "./examples/wikiV.txt"
    val name = "test"
    val attrs = Seq("a", "b")

    val relation1 = RelationSchema(name, attrs)


    println(s"element of relation-${name} is ${relation1.load(dataAdress).content.count()}")

    SparkSingle.close()
  }

}
