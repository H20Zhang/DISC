package TestData

import org.apache.spark.Logo.Logical.LogoEdgePatternBuildLogicalStep

object TestLogoRDDReferenceData {
  lazy val edgeLogoRDDReference = new LogoEdgePatternBuildLogicalStep(TestLogoRDDData.debugEdgePatternLogoRDD) toLogoRDDReference()
}
