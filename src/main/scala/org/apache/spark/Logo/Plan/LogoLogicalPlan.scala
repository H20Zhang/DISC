package org.apache.spark.Logo.Plan


import org.apache.spark.Logo.UnderLying.Joiner.LogoBuildScriptStep
import org.apache.spark.Logo.UnderLying.dataStructure.{PatternInstance, _}
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}


class FilteringCondition(var f: PatternInstance => Boolean, var isStrictCondition: Boolean, var nodesNotSameTuple:Array[(Int,Int)]) extends Serializable {
  override def clone(): AnyRef = {
    new FilteringCondition(f, isStrictCondition,nodesNotSameTuple)
  }
}

object FilteringCondition{
  def apply(f: PatternInstance => Boolean,isStrictCondition: Boolean, nodesNotSameTuple:Array[(Int,Int)]=null): FilteringCondition = {
      new FilteringCondition(f,isStrictCondition,nodesNotSameTuple)
  }
}

//object FilteringCondition{
//  def apply(f: PatternInstance => Boolean, isStrictCondition: Boolean
//  ): FilteringCondition = new FilteringCondition(f, isStrictCondition)
//}

/**
  * Represent a logical reference to Logo, an wrapper around LogoBuildScriptStep
  *
  * @param schema          the schema of the logo
  * @param buildScriptStep the buildScript used to build the new Logo
  */
abstract class LogoRDDReference(schema: LogoSchema, buildScriptStep: LogoBuildScriptStep) {
  def generateF(): PatternLogoRDD

  def generateJ(): ConcreteLogoRDD

  def optimize(): Unit

  def size(): Long

  def blockCount(): Long

  def rdd():RDD[PatternInstance]

//  def join(pattern1:PatternLogoRDDReference, pattern2:PatternLogoRDDReference, joinAttr1:String, joinAttr2:String):ComposingPatternLogoRDDReference
//
//  def join(pattern1:PatternLogoRDDReference, joinAttr:String):ComposingPatternLogoRDDReference

  //filter patterns using a f function, and specify what state the filtered function should be in.
  def filter(f: PatternInstance => Boolean, isStrict:Boolean = false): PatternLogoRDDReference

  //filter a pattern using a Filtering Condition.
  def filter(f: FilteringCondition): PatternLogoRDDReference

  //manual assign a pattern to be in J mode, the default mode of pattern is F mode.
  //(J equals to eager mode(pattern materialized), J equals to lazy mode(local join is delayed))
  def toConcrete():PatternLogoRDDReference

  //build a larger pattern using two sub-pattern, this works like a Binary Join.
  def build(subPattern: SubPatternLogoRDDReference): ComposingPatternLogoRDDReference

  //build a larger pattern using three sub-pattern, this works like a GJ Join.
  def build(subPattern1: SubPatternLogoRDDReference, subPattern2: SubPatternLogoRDDReference): ComposingPatternLogoRDDReference

  //build a larger pattern using four sub-pattern, this works like a GJ Join.
  def build(subPattern1: SubPatternLogoRDDReference, subPattern2: SubPatternLogoRDDReference, subPattern3: SubPatternLogoRDDReference): ComposingPatternLogoRDDReference
}

/**
  * Represent a logical reference to PatternLogo, an wrapper around LogoPatternBuildLogicalStep
  *
  * @param patternSchema the schema of the patternLogo
  * @param buildScript   the buildScript used to build the new Logo
  */
class PatternLogoRDDReference(val patternSchema: LogoSchema, var buildScript: LogoPatternPhysicalPlan) extends LogoRDDReference(patternSchema, buildScript) {


  var filteringCondition: FilteringCondition = null

  //  def unsetFilterCondition:PatternLogoRDDReference = {
  //    filteringCondition = null
  //    this
  //  }
  //
  //  //TODO: finish filtering
  //  def setFilteringCondition(f:FilteringCondition):PatternLogoRDDReference = {
  //    this.filteringCondition = f
  //    this
  //  }
  //


  def executeFiltering(f: FilteringCondition): PatternLogoRDDReference = {
    val filteredBuildScript = new LogoFilterPatternPhysicalPlan(f, buildScript)
    val newData = filteredBuildScript.generateNewPatternJState()
    val newBuildScript = new LogoEdgePatternPhysicalPlan(newData)
    new PatternLogoRDDReference(patternSchema, newBuildScript)
  }

  def filter(f: FilteringCondition): PatternLogoRDDReference = {
    val filteredBuildScript = new LogoFilterPatternPhysicalPlan(f, buildScript)
    if (f.isStrictCondition) {


      val newBuildScript = new LogoEdgePatternPhysicalPlan(filteredBuildScript.generateNewPatternJState())
      new PatternLogoRDDReference(patternSchema, newBuildScript)
    } else {
      new PatternLogoRDDReference(patternSchema, filteredBuildScript)
    }
  }

  override def toConcrete():PatternLogoRDDReference = {
    val newBuildScript = new LogoEdgePatternPhysicalPlan(buildScript.generateNewPatternJState())
    new PatternLogoRDDReference(patternSchema, newBuildScript)
  }

  def toKeyValue(key:Set[Int],isSorted:Boolean=true):PatternLogoRDDReference = {
    val newBuildScript = new LogoKeyValuePatternPhysicalPlan(buildScript.generateNewPatternJState().toKeyValuePatternLogoRDD(key,isSorted))
    new PatternLogoRDDReference(patternSchema, newBuildScript)
  }

  def toIdentitySubPattern(): SubPatternLogoRDDReference = new SubPatternLogoRDDReference(this, KeyMapping(List.range(0, patternSchema.nodeSize)))

  //prepare for build operation.
  def toSubPattern(keyMapping: KeyMapping): SubPatternLogoRDDReference = new SubPatternLogoRDDReference(this, keyMapping)

  def toSubPattern(keyMapping: (Int, Int)*): SubPatternLogoRDDReference = {
    new SubPatternLogoRDDReference(this, KeyMapping(keyMapping.toMap))
  }

  def to(keyMapping: Int*): SubPatternLogoRDDReference = {
    new SubPatternLogoRDDReference(this, KeyMapping(keyMapping.zipWithIndex.map(f => (f._2,f._1)).toMap))
  }


  //generate the F-state Logo
  override def generateF() = {
    optimize()
    buildScript.generateNewPatternFState()
  }

  //generate the J-state Logo
  override def generateJ() = {
    optimize()
    buildScript.generateNewPatternJState()
  }

  override def optimize(): Unit = {
    //    buildScript.setFilteringCondition(filteringCondition)
  }

  override def rdd() = {
    new MapPartitionsRDD[PatternInstance,LogoBlockRef](generateF().logoRDD,
      {(context, pid, iter) =>
        val block = iter.next().asInstanceOf[PatternLogoBlock[_]]
        block.enumerateIterator()
      }
    )
  }

  override def size(): Long = {
    generateF().logoRDD.map {
      f =>
        var size = 0L
        val block = f.asInstanceOf[PatternLogoBlock[_]]

        val iterator = block.enumerateIterator()

        val time = System.nanoTime()

        if (iterator.isInstanceOf[enumerateIterator]){
          size += iterator.asInstanceOf[enumerateIterator].longSize()
        } else{
          while (iterator.hasNext) {
            size += 1
          }
        }

        val time2 = System.nanoTime()

        println(s"computation time is: ${(time2-time).toDouble/1000000000}")

        size
    }.sum().toLong
  }

  override def blockCount(): Long = {
    generateF().logoRDD.map {
      f =>
        var size = 0L
        val block = f.asInstanceOf[PatternLogoBlock[_]]
        val iterator = block.enumerateIterator()

        while (iterator.hasNext) {
          iterator.next()
          size = size + 1
        }
        size

    }.count()
  }

  override def build(subPattern: SubPatternLogoRDDReference): ComposingPatternLogoRDDReference = {
    this.toIdentitySubPattern().build(subPattern)
  }

  override def build(subPattern1: SubPatternLogoRDDReference, subPattern2: SubPatternLogoRDDReference): ComposingPatternLogoRDDReference = {
    this.toIdentitySubPattern().build(subPattern1,subPattern2)
  }

  override def build(subPattern1: SubPatternLogoRDDReference, subPattern2: SubPatternLogoRDDReference, subPattern3: SubPatternLogoRDDReference): ComposingPatternLogoRDDReference = {
    this.toIdentitySubPattern().build(subPattern1,subPattern2,subPattern3)
  }




  override def filter(f: PatternInstance => Boolean, isStrict:Boolean = false): PatternLogoRDDReference = {

    val filteringCondition = FilteringCondition(f, isStrict)

    this.filter(filteringCondition)
  }

//  override def join(pattern1: PatternLogoRDDReference, pattern2: PatternLogoRDDReference, joinAttr1: String, joinAttr2: String): ComposingPatternLogoRDDReference = ???
//
//  override def join(pattern1: PatternLogoRDDReference, joinAttr: String): ComposingPatternLogoRDDReference = {
//    val schema = patternSchema
//    val nextEle = schema.keyCol.size
//
//    val schema1 = pattern1.patternSchema
//
//    val joinAttrs = joinAttr.split(";").map(f => f.split("on")).map(f => (f(0).toInt, f(1).toInt)).toMap
//    pattern1.toSubPattern(KeyMapping(joinAttrs))
//
//
//  }
}

/**
  * The class that represent the logical plan of composing a new pattern using the two old pattern
  *
  * @param children           the old pattern used to construct the new pattern
  * @param patternSchema      the schema of the patternLogo
  * @param composebuildScript the buildScript used to build the new Logo
  */
class ComposingPatternLogoRDDReference(val children: Seq[PatternLogoRDDReference], override val patternSchema: LogoSchema, var composebuildScript: LogoPatternPhysicalPlan) extends PatternLogoRDDReference(patternSchema, composebuildScript) {

}


/**
  * a convenient class for building Logo
  *
  * @param patternLogoRDDReference the old LogoReference
  * @param keyMapping              the keyMapping that map the old Logo to new Logo
  */
class SubPatternLogoRDDReference(val patternLogoRDDReference: PatternLogoRDDReference, val keyMapping: KeyMapping) {

  def build(subPattern: SubPatternLogoRDDReference): ComposingPatternLogoRDDReference = {
    val logoBuildScriptSteps = Seq(patternLogoRDDReference.buildScript, subPattern.patternLogoRDDReference.buildScript)
    val keyMappings = Seq(keyMapping, subPattern.keyMapping)
    val newLogoBuildScriptStep = new LogoComposite2PatternPhysicalPlan(logoBuildScriptSteps, keyMappings)
    val logoRDDReference = newLogoBuildScriptStep.toLogoRDDReference()

    new ComposingPatternLogoRDDReference(Seq(patternLogoRDDReference, subPattern.patternLogoRDDReference), logoRDDReference.patternSchema, logoRDDReference.buildScript)
  }

  //TODO this only intended for pattern like square, more cases need to be further implemented
  def build(subPattern1: SubPatternLogoRDDReference, subPattern2: SubPatternLogoRDDReference): ComposingPatternLogoRDDReference = {
    val logoBuildScriptSteps = Seq(patternLogoRDDReference.buildScript, subPattern1.patternLogoRDDReference.buildScript, subPattern2.patternLogoRDDReference.buildScript)
    val keyMappings = Seq(keyMapping, subPattern1.keyMapping, subPattern2.keyMapping)
    val newLogoBuildScriptStep = new LogoComposite3IntersectionPatternPhysicalPlan(logoBuildScriptSteps, keyMappings)
    val logoRDDReference = newLogoBuildScriptStep.toLogoRDDReference()

    new ComposingPatternLogoRDDReference(Seq(patternLogoRDDReference, subPattern1.patternLogoRDDReference, subPattern2.patternLogoRDDReference), logoRDDReference.patternSchema, logoRDDReference.buildScript)
  }

  //TODO this only intended for pattern like fourClique, more cases need to be further implemented
  def build(subPattern1: SubPatternLogoRDDReference, subPattern2: SubPatternLogoRDDReference, subPattern3: SubPatternLogoRDDReference): ComposingPatternLogoRDDReference = {
    val logoBuildScriptSteps = Seq(patternLogoRDDReference.buildScript, subPattern1.patternLogoRDDReference.buildScript, subPattern2.patternLogoRDDReference.buildScript, subPattern3.patternLogoRDDReference.buildScript)
    val keyMappings = Seq(keyMapping, subPattern1.keyMapping, subPattern2.keyMapping, subPattern3.patternLogoRDDReference.buildScript)
    val newLogoBuildScriptStep = new LogoComposite4IntersectionPatternPhysicalPlan(logoBuildScriptSteps, keyMappings)
    val logoRDDReference = newLogoBuildScriptStep.toLogoRDDReference()

    new ComposingPatternLogoRDDReference(Seq(patternLogoRDDReference, subPattern1.patternLogoRDDReference, subPattern2.patternLogoRDDReference, subPattern3.patternLogoRDDReference), logoRDDReference.patternSchema, logoRDDReference.buildScript)
  }
}





