import TestData.TestLogoRDDData
import org.apache.spark.Logo.Physical.Builder.{Catalog, LogoBuildScriptOneStep, LogoRDDReference, SnapPoint}
import org.apache.spark.Logo.Physical.dataStructure.{LogoBlockRef, RowLogoBlock}
import org.apache.spark.Logo.Physical.utlis.{SparkSingle, TestUtil}
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class BuilderTest extends FunSuite with BeforeAndAfterAll{

  test("catalog"){

    val (edgeLogoRDD,schema) = TestLogoRDDData.edgeLogoRDD

    val ref = LogoRDDReference(edgeLogoRDD.asInstanceOf[RDD[LogoBlockRef]],schema)

    //put test
    Catalog.putLogo("edge",ref)

    //get test
    val edgeLogoRDD1 = Catalog.getLogo[RDD[RowLogoBlock[(scala.List[Int], Int)]]]("edge")

    assert(TestUtil.objectEqual(edgeLogoRDD,edgeLogoRDD1))


    //delete test
    Catalog.removeLogo("edge")

    var ref2:LogoRDDReference = null

    try{
      ref2 = Catalog.getLogo("edge")


    } catch {
      case _ => {}
    }

    assert(ref2 == null)
  }

  test("logoBuildScript"){
    val sc = SparkSingle.getSparkContext()

    val (edgeLogoRDD,schema) = TestLogoRDDData.edgeLogoRDD

    val edgeRef0 = LogoRDDReference(edgeLogoRDD.asInstanceOf[RDD[LogoBlockRef]],schema)
    val edgeRef1 = LogoRDDReference(edgeLogoRDD.asInstanceOf[RDD[LogoBlockRef]],schema)
    val edgeRef2 = LogoRDDReference(edgeLogoRDD.asInstanceOf[RDD[LogoBlockRef]],schema)

    val logoRDDRefs = List(edgeRef0,edgeRef1,edgeRef2)
    val snapPoints = List(
      SnapPoint(0,0,1,0),
      SnapPoint(1,1,2,1)
    )

    val handler = (blocks:Seq[LogoBlockRef]) => {
      blocks.foreach { f =>
        println(f.asInstanceOf[RowLogoBlock[(scala.Seq[Int], Int)]].metaData.numberOfParts)
      }
      blocks(0)
    }


    //logoScriptOneStep
    //Compiling Test
    val oneStep = LogoBuildScriptOneStep(logoRDDRefs,snapPoints,handler)

    println("keymapping is:")
    println(oneStep.compositeSchema.keyMapping)
    val fetchJoinRDD = oneStep.performFetchJoin(sc)

    //TODO This requires further testing
    fetchJoinRDD.count()


  }


}
