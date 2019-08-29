package leapfrog

import org.apache.spark.adj.database.Catalog
import org.apache.spark.adj.leapfrog.LeapFrog
import org.apache.spark.adj.utils.SparkSingle
import org.apache.spark.adj.utils.testing.{LeapFrogJoinValidator, QueryGenerator, TestingHelper, TestingSubJoins}
import org.apache.spark.sql.AnalysisException
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class LeapFrogTest extends FunSuite with BeforeAndAfterAll{


  test("prepare testing data"){

    val graphRelation = TestingHelper.genGraphContent("debug")
    println(graphRelation.size)
    println(graphRelation.toSeq.map(_.toSeq))

//    val subjoins1 = TestingData.testing_subjoins2
//    println(subjoins1)
//    println(subjoins1.blocks.map(_.content.toSeq.map(_.toSeq)))
  }

  test("debug internal"){
    val subJoins1 = TestingSubJoins.testing_subjoins1
    val leapfrog = new LeapFrog(subJoins1)
    leapfrog.debugInternal()
  }

  test("leapfrog -- graph"){

    val subJoins = TestingSubJoins.testing_subjoins2
    val leapfrog = new LeapFrog(subJoins)
    println(leapfrog.tries.size)
    leapfrog.relevantRelationForAttrMap.toSeq.map(f => f.map(x => (x._1, x._2, x._3.toSeq, x._4))).foreach(println)

    assert(leapfrog.size == 3650334)
  }

  test("leapfrog -- random query"){

    val spark = SparkSingle.getSparkSession()
    val numRelation = 4
    val arity = 4
    val cardinality = 1000
    val testRun = 100

    Range(0, testRun).foreach{
      id =>

        val (contents, schemas) = QueryGenerator.genRandomQuery(numRelation, arity, cardinality)
        val validator = new LeapFrogJoinValidator(contents, schemas)

        try{
          assert(validator.validate())
        } catch {
          case e:AnalysisException =>
          case _ => assert(false)
        }
    }
  }

  test("leapfrog -- sparksql"){
    val spark = SparkSingle.getSparkSession()
    import spark.implicits._
//    spark.createDataset(Array(1,2, 3,4)).count()

    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.count()
  }

  override protected def afterAll(): Unit = {
    SparkSingle.close()
  }



}
