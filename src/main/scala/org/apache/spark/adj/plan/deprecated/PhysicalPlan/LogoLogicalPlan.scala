package org.apache.spark.adj.plan.deprecated.PhysicalPlan

import org.apache.spark.adj.execution.hypercube.LogoPhysicalPlan
import org.apache.spark.adj.execution.rdd.{PatternInstance, _}
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
abstract class AbstractLogo(schema: LogoSchema, buildScriptStep: LogoPhysicalPlan) {
  def generateF(): PatternLogoRDD

  def generateJ(): ConcreteLogoRDD

  def optimize(): Unit

  def size(): Long

  def approximateSize(): Long

  def blockCount(): Long

  def rdd():RDD[PatternInstance]

//  def join(pattern1:PatternLogoRDDReference, pattern2:PatternLogoRDDReference, joinAttr1:String, joinAttr2:String):ComposingPatternLogoRDDReference
//
//  def join(pattern1:PatternLogoRDDReference, joinAttr:String):ComposingPatternLogoRDDReference

  //filter patterns using a f function, and specify what state the filtered function should be in.
  def filter(f: PatternInstance => Boolean, isStrict:Boolean = false): Logo

  //filter a pattern using a Filtering Condition.
  def filter(f: FilteringCondition): Logo

  //manual assign a pattern to be in J mode, the default mode of pattern is F mode.
  //(J equals to eager mode(pattern materialized), J equals to lazy mode(local join is delayed))
  def toConcrete():Logo

  //build a larger pattern using two sub-pattern, this works like a Binary Join.
  def build(subPattern: PartialLogo): ComposedLogo

  //build a larger pattern using three sub-pattern, this works like a GJ Join.
  def build(subPattern1: PartialLogo, subPattern2: PartialLogo): ComposedLogo

  //build a larger pattern using four sub-pattern, this works like a GJ Join.
  def build(subPattern1: PartialLogo, subPattern2: PartialLogo, subPattern3: PartialLogo): ComposedLogo
}

/**
  * Represent a logical reference to PatternLogo, an wrapper around LogoPatternBuildLogicalStep
  *
  * @param patternSchema the schema of the patternLogo
  * @param buildScript   the buildScript used to build the new Logo
  */
class Logo(val patternSchema: LogoSchema, var buildScript: AbstractLogoPhysicalPlan) extends AbstractLogo(patternSchema, buildScript) {


  var filteringCondition: FilteringCondition = null

  def executeFiltering(f: FilteringCondition): Logo = {
    val filteredBuildScript = new LogoFilterPatternPhysicalPlan(f, buildScript)
    val newData = filteredBuildScript.generateNewPatternJState()
    val newBuildScript = new LogoEdgePhysicalPlan(newData)
    new Logo(patternSchema, newBuildScript)
  }

  def filter(f: FilteringCondition): Logo = {
    val filteredBuildScript = new LogoFilterPatternPhysicalPlan(f, buildScript)
    if (f.isStrictCondition) {


      val newBuildScript = new LogoEdgePhysicalPlan(filteredBuildScript.generateNewPatternJState())
      new Logo(patternSchema, newBuildScript)
    } else {
      new Logo(patternSchema, filteredBuildScript)
    }
  }

  override def toConcrete():Logo = {
    val newBuildScript = new LogoEdgePhysicalPlan(buildScript.generateNewPatternJState())
    new Logo(patternSchema, newBuildScript)
  }

  def toKeyValue(key:Set[Int],isSorted:Boolean=true):Logo = {



    val newBuildScript = new LogoKeyValuePhysicalPlan(buildScript.generateNewPatternFState().toKeyValuePatternLogoRDD(key,isSorted))
    new Logo(patternSchema, newBuildScript)
  }

  def toIdentitySubPattern(): PartialLogo = new PartialLogo(this, KeyMapping(List.range(0, patternSchema.nodeSize)))

  //prepare for build operation.
  def toSubPattern(keyMapping: KeyMapping): PartialLogo = new PartialLogo(this, keyMapping)

  def toSubPattern(keyMapping: (Int, Int)*): PartialLogo = {
    new PartialLogo(this, KeyMapping(keyMapping.toMap))
  }

  def to(keyMapping: Int*): PartialLogo = {
    new PartialLogo(this, KeyMapping(keyMapping.zipWithIndex.map(f => (f._2,f._1)).toMap))
  }

  def toWithSeqKeyMapping(keyMapping: Seq[Int]): PartialLogo = {
    new PartialLogo(this, KeyMapping(keyMapping.zipWithIndex.map(f => (f._2,f._1)).toMap))
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

  var cachedRDD:RDD[LogoBlockRef] = null

  def cache(): Unit = {
    cachedRDD =  generateF().logoRDD.cache()
  }

  private def logoRDD():RDD[LogoBlockRef] = {
    if (cachedRDD != null){
      cachedRDD
    } else{
      generateF().logoRDD
    }
  }

  override def rdd() = {
    new MapPartitionsRDD[PatternInstance,LogoBlockRef](logoRDD(),
      {(context, pid, iter) =>
        val block = iter.next().asInstanceOf[PatternLogoBlock[_]]
        block.enumerateIterator()
      }
    )
  }

  def ForLoopSize(): Long = {
    logoRDD().map {
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

//        println(s"computation time is: ${(time2-time).toDouble/1000000000}")


//        val r = scala.util.Random

        var count = size
        var zero = 145.0
        while(count > 0){
          count = count - 1
          zero = zero / 123.2
          zero = zero+count
//          if (size == 0){
//            zero += 1
//          }
        }



        (zero,size)
    }.map(_._1).sum().toLong
  }

  override def size(): Long = {
    logoRDD().map {
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

        size
    }.sum().toLong
  }

  def time_size() = {
    val time_size_pair = logoRDD().map {
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
        val duration = (time2 - time)/1000000

        //        println(s"computation time is: ${(time2-time).toDouble/1000000000}")

        (size,duration)
    }.collect()

    (time_size_pair.map(f => f._1).sum,time_size_pair.map(f => f._2).sum,time_size_pair.size)
  }

  override def approximateSize(): Long = {
    generateF().logoRDD.map {
      f =>
        var size = 0L
        val block = f.asInstanceOf[PatternLogoBlock[_]]

        val iterator = block.enumerateIterator()

        val time = System.nanoTime()

        if (iterator.isInstanceOf[enumerateIterator]){
          size += iterator.asInstanceOf[enumerateIterator].approximateLongSize()
        } else{
          while (iterator.hasNext) {
            size += 1
          }
        }

        val time2 = System.nanoTime()

        //        println(s"computation time is: ${(time2-time).toDouble/1000000000}")

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

  def count(): Long = {
    generateF().logoRDD.map {
      f =>
        var size = 0L
        val block = f.asInstanceOf[PatternLogoBlock[_]]
        val iterator = block.enumerateIterator()
        1
    }.count()
  }

  override def build(subPattern: PartialLogo): ComposedLogo = {
    this.toIdentitySubPattern().build(subPattern)
  }

  override def build(subPattern1: PartialLogo, subPattern2: PartialLogo): ComposedLogo = {
    this.toIdentitySubPattern().build(subPattern1,subPattern2)
  }

  def gSyncbuild(subPattern1: PartialLogo, subPattern2: PartialLogo): ComposedLogo = {
    this.toIdentitySubPattern().gSyncbuild(subPattern1,subPattern2)
  }

//  def gSyncbuild(subPattern1: SubPatternLogoRDDReference, subPattern2: SubPatternLogoRDDReference, subPattern3: SubPatternLogoRDDReference): ComposingPatternLogoRDDReference = {
//    this.toIdentitySubPattern().gSyncbuild(subPattern1,subPattern2,subPattern3)
//  }

  override def build(subPattern1: PartialLogo, subPattern2: PartialLogo, subPattern3: PartialLogo): ComposedLogo = {
    this.toIdentitySubPattern().build(subPattern1,subPattern2,subPattern3)
  }




  override def filter(f: PatternInstance => Boolean, isStrict:Boolean = false): Logo = {

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
class ComposedLogo(val children: Seq[Logo], override val patternSchema: LogoSchema, var composebuildScript: AbstractLogoPhysicalPlan) extends Logo(patternSchema, composebuildScript) {

}


/**
  * a convenient class for building Logo
  *
  * @param patternLogoRDDReference the old LogoReference
  * @param keyMapping              the keyMapping that map the old Logo to new Logo
  */
class PartialLogo(val patternLogoRDDReference: Logo, val keyMapping: KeyMapping) {

  def build(subPattern: PartialLogo): ComposedLogo = {
    val logoBuildScriptSteps = Seq(patternLogoRDDReference.buildScript, subPattern.patternLogoRDDReference.buildScript)
    val keyMappings = Seq(keyMapping, subPattern.keyMapping)
    val newLogoBuildScriptStep = new LogoComposite2PhysicalPlan(logoBuildScriptSteps, keyMappings)
    val logoRDDReference = newLogoBuildScriptStep.toLogoRDDReference()

    new ComposedLogo(Seq(patternLogoRDDReference, subPattern.patternLogoRDDReference), logoRDDReference.patternSchema, logoRDDReference.buildScript)
  }

  //TODO this only intended for pattern like square, more cases need to be further implemented
  def build(subPattern1: PartialLogo, subPattern2: PartialLogo): ComposedLogo = {
    val logoBuildScriptSteps = Seq(patternLogoRDDReference.buildScript, subPattern1.patternLogoRDDReference.buildScript, subPattern2.patternLogoRDDReference.buildScript)
    val keyMappings = Seq(keyMapping, subPattern1.keyMapping, subPattern2.keyMapping)
    val newLogoBuildScriptStep = new LogoComposite3PhysicalPlan(logoBuildScriptSteps, keyMappings)
    val logoRDDReference = newLogoBuildScriptStep.toLogoRDDReference()

    new ComposedLogo(Seq(patternLogoRDDReference, subPattern1.patternLogoRDDReference, subPattern2.patternLogoRDDReference), logoRDDReference.patternSchema, logoRDDReference.buildScript)
  }

  //TODO this only intended for pattern like fourClique, more cases need to be further implemented
  def build(subPattern1: PartialLogo, subPattern2: PartialLogo, subPattern3: PartialLogo): ComposedLogo = {
    val logoBuildScriptSteps = Seq(patternLogoRDDReference.buildScript, subPattern1.patternLogoRDDReference.buildScript, subPattern2.patternLogoRDDReference.buildScript, subPattern3.patternLogoRDDReference.buildScript)
    val keyMappings = Seq(keyMapping, subPattern1.keyMapping, subPattern2.keyMapping, subPattern3.keyMapping)
    val newLogoBuildScriptStep = new LogoComposite4PhysicalPlan(logoBuildScriptSteps, keyMappings)
    val logoRDDReference = newLogoBuildScriptStep.toLogoRDDReference()

    new ComposedLogo(Seq(patternLogoRDDReference, subPattern1.patternLogoRDDReference, subPattern2.patternLogoRDDReference, subPattern3.patternLogoRDDReference), logoRDDReference.patternSchema, logoRDDReference.buildScript)
  }

  def gSyncbuild(subPattern1: PartialLogo, subPattern2: PartialLogo): ComposedLogo = {
    val logoBuildScriptSteps = Seq(patternLogoRDDReference.buildScript, subPattern1.patternLogoRDDReference.buildScript, subPattern2.patternLogoRDDReference.buildScript)
    val keyMappings = Seq(keyMapping, subPattern1.keyMapping, subPattern2.keyMapping)
    val newLogoBuildScriptStep = new LogoComposite3PhysicalPlan(logoBuildScriptSteps, keyMappings, true)
    val logoRDDReference = newLogoBuildScriptStep.toLogoRDDReference()

    new ComposedLogo(Seq(patternLogoRDDReference, subPattern1.patternLogoRDDReference, subPattern2.patternLogoRDDReference), logoRDDReference.patternSchema, logoRDDReference.buildScript)
  }

//  def gSyncbuild(subPattern1: SubPatternLogoRDDReference, subPattern2: SubPatternLogoRDDReference, subPattern3: SubPatternLogoRDDReference): ComposingPatternLogoRDDReference = {
//    val logoBuildScriptSteps = Seq(patternLogoRDDReference.buildScript, subPattern1.patternLogoRDDReference.buildScript, subPattern2.patternLogoRDDReference.buildScript, subPattern3.patternLogoRDDReference.buildScript)
//    val keyMappings = Seq(keyMapping, subPattern1.keyMapping, subPattern2.keyMapping,subPattern3.keyMapping)
//    val newLogoBuildScriptStep = new LogoComposite4IntersectionPatternPhysicalPlan(logoBuildScriptSteps, keyMappings, true)
//    val logoRDDReference = newLogoBuildScriptStep.toLogoRDDReference()
//
//    new ComposingPatternLogoRDDReference(Seq(patternLogoRDDReference, subPattern1.patternLogoRDDReference, subPattern2.patternLogoRDDReference), logoRDDReference.patternSchema, logoRDDReference.buildScript)
//  }
}





