import TestData.TestLogoRDDData
import org.apache.spark.Logo.Physical.Builder.{Catalog, LogoRDDReference, RowLogoRDDReference}
import org.apache.spark.Logo.Physical.dataStructure.{LogoBlockRef, RowLogoBlock}
import org.apache.spark.Logo.Physical.utlis.TestUtil
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class BuilderTest extends FunSuite with BeforeAndAfterAll{

  test("catalog"){

    val (edgeLogoRDD,schema) = TestLogoRDDData.edgeLogoRDD

    val ref = RowLogoRDDReference(edgeLogoRDD.asInstanceOf[RDD[LogoBlockRef]],schema)

    //put test
    Catalog.putLogo("edge",ref)

    //get test
    val ref1 = Catalog.getLogo("edge")
    assert(TestUtil.objectEqual(ref,ref1))

    //type transtion
    val edge = ref1 match {
      case r: RowLogoRDDReference => r.logoRDD
    }

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


}
