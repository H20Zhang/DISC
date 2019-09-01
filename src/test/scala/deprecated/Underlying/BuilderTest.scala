package deprecated.Underlying

import hzhang.test.exp.data.TestLogoRDDData
import org.apache.spark.adj.deprecated.execution.catalog.Catalog
import org.apache.spark.adj.deprecated.hypercube.{
  LogoBuildPhyiscalStep,
  SnapPoint
}
import org.apache.spark.adj.deprecated.execution.rdd._
import org.apache.spark.adj.deprecated.utlis.TestUtil
import org.apache.spark.adj.utils.misc.SparkSingle
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class BuilderTest extends FunSuite with BeforeAndAfterAll {

  test("catalog") {

    val (edgeLogoRDD, schema) = TestLogoRDDData.EdgeRowLogoRDD

    val ref = new LogoRDD(edgeLogoRDD.asInstanceOf[RDD[LogoBlockRef]], schema)

    //put test
    Catalog.putLogo("edge", ref)

    //get test
    val edgeLogoRDD1 =
      Catalog.getLogo[RDD[RowLogoBlock[(scala.List[Int], Int)]]]("edge")

    assert(TestUtil.objectEqual(edgeLogoRDD, edgeLogoRDD1))

    //delete test
    Catalog.removeLogo("edge")

    var ref2: LogoRDD = null

    try {
      ref2 = Catalog.getLogo("edge")

    } catch {
      case _ => {}
    }

    assert(ref2 == null)
  }

  test("triangleCounting") {
    val sc = SparkSingle.getSparkContext()

    val (edgeLogoRDD, schema) = TestLogoRDDData.EdgeRowLogoRDD

    val edgeRef0 =
      new LogoRDD(edgeLogoRDD.asInstanceOf[RDD[LogoBlockRef]], schema)
    val edgeRef1 =
      new LogoRDD(edgeLogoRDD.asInstanceOf[RDD[LogoBlockRef]], schema)
    val edgeRef2 =
      new LogoRDD(edgeLogoRDD.asInstanceOf[RDD[LogoBlockRef]], schema)

    val logoRDDRefs = List(edgeRef0, edgeRef1, edgeRef2)
    val snapPoints =
      List(SnapPoint(0, 0, 1, 0), SnapPoint(1, 1, 2, 1), SnapPoint(0, 1, 2, 0))

    val handler =
      (blocks: Seq[LogoBlockRef], schema: CompositeLogoSchema, index: Int) => {

        val edgeBlock0 = blocks(0).asInstanceOf[RowLogoBlock[(Seq[Int], Int)]]
        val edgeBlock1 = blocks(1).asInstanceOf[RowLogoBlock[(Seq[Int], Int)]]
        val edgeBlock2 = blocks(2).asInstanceOf[RowLogoBlock[(Seq[Int], Int)]]

        //TODO Debug
        println("***print***")
        println(schema)
        println(edgeBlock0.metaData)
        println(edgeBlock1.metaData)
        println(edgeBlock2.metaData)

        val edge0 = edgeBlock0.rawData
          .map(f => (f._1(0), f._1(1)))
          .groupBy(f => f._1)
          .map(f => (f._1, f._2.map(_._2)))
          .toMap

        //need to swap the src and dst
        val edge2 = edgeBlock2.rawData
          .map(f => (f._1(1), f._1(0)))
          .groupBy(f => f._1)
          .map(f => (f._1, f._2.map(_._2)))
          .toMap

        val edge1 = edgeBlock1.rawData.map(f => (f._1))

        val triangleSize = edge1.map { f =>
          var res = 0L

          if (f(0) < f(1)) {
            if (edge0.contains(f(0)) && edge2.contains(f(1))) {
              val leftNeighbors = edge0(f(0))
              val rightNeighbors = edge2(f(1))
              val intersectList = leftNeighbors.intersect(rightNeighbors)

              res = intersectList.filter(p => p > f(1)).size.toLong
            }
          }

          res
        }

        println(triangleSize.sum)
        new CountLogo(triangleSize.sum)
      }

    val compositeSchema = new IntersectionCompositeLogoSchemaBuilder(
      logoRDDRefs.map(_.schema),
      snapPoints
    ) generate ()

    //logoScriptOneStep
    //Compiling Test
    val oneStep =
      LogoBuildPhyiscalStep(logoRDDRefs, compositeSchema, handler, "Triangle")

    println("keymapping is:")
    println(oneStep.compositeSchema.keyMappings)
    val fetchJoinRDD = oneStep.performFetchJoin(sc)

    //TODO This requires further testing
    val triangleCount = fetchJoinRDD
      .map { f =>
        val countLogo = f.asInstanceOf[CountLogo]
        countLogo.count
      }
      .sum()

    println(triangleCount)

  }

  test("test building triangle") {}

  test("test building chordal square") {}

  test("test building three triangle") {}

  test("test building chordal roof") {}

}
