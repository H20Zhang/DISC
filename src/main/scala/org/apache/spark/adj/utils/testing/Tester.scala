package org.apache.spark.adj.utils.testing

import breeze.util.ArrayUtil
import org.apache.spark.adj.database.Catalog
import org.apache.spark.sql.AnalysisException
import org.bouncycastle.util.test.TestFailedException

abstract class Tester(numRelation: Int,
                      arity: Int,
                      cardinality: Int,
                      testRun: Int) {

  def testRuns()
}

class HCubeTester(numRelation: Int, arity: Int, cardinality: Int, testRun: Int)
    extends Tester(numRelation, arity, cardinality, testRun) {

  def testRuns() = {
    Range(0, testRun).foreach { id =>
      val (contents, schemas) =
        QueryGenerator.genRandomQuery(numRelation, arity, cardinality)

      //      val (contents, schemas) = QueryGenerator.genDebugSubJoin()
      val validator = new HCubeJoinValidator(contents, schemas)

      try {
        assert(validator.validate())
      } catch {
        case e: AnalysisException   =>
        case e: TestFailedException => throw e
        case e: Exception           => throw e
      }

      Catalog.reset()
    }
  }
}

class LeapFrogUnaryTester(numRelation: Int, cardinality: Int, testRun: Int)
    extends Tester(numRelation, 1, cardinality, testRun) {
  override def testRuns(): Unit = {
    Range(0, testRun).foreach { id =>
      val (contents, schemas) =
        QueryGenerator.genRandomUnaryRelationQuery(numRelation, cardinality)

      contents.foreach { f =>
        java.util.Arrays.sort(f)
//        println(f.toSeq)
      }
//      println(contents.map(_.toSeq))

      //      val (contents, schemas) = QueryGenerator.genDebugSubJoin()
      val validator = new LeapFrogUnaryValidator(contents, schemas)

      try {
        assert(validator.validate())
      } catch {
        case e: AnalysisException   =>
        case e: TestFailedException => throw e
        case e: Exception           => throw e
      }

      Catalog.reset()
    }
  }
}
