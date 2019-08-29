package org.apache.spark.adj.deprecated.utlis

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.adj.deprecated.execution.rdd._
import org.apache.spark.adj.deprecated.plan.deprecated.PhysicalPlan.FilteringCondition
import org.apache.spark.serializer.KryoRegistrator

import scala.collection.mutable


class KryoRegistor extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[mutable.LongMap[Array[ValuePatternInstance]]], new LongMapSerializer())
    kryo.register(classOf[mutable.LongMap[CompactPatternList]], new LongMapCompactSerializer())
    kryo.register(classOf[LogoSchema])
    kryo.register(classOf[CompositeLogoSchema])
    kryo.register(classOf[LogoMetaData])
    kryo.register(classOf[ConcretePatternLogoBlock])
    kryo.register(classOf[KeyValuePatternLogoBlock])
    kryo.register(classOf[CompositeTwoPatternLogoBlock])
    kryo.register(classOf[FilteringPatternLogoBlock[_]])
    kryo.register(classOf[PatternInstance])
    kryo.register(classOf[OneKeyPatternInstance])
    kryo.register(classOf[TwoKeyPatternInstance])
    kryo.register(classOf[ValuePatternInstance])
    kryo.register(classOf[OneValuePatternInstance])
    kryo.register(classOf[TwoValuePatternInstance])
    kryo.register(classOf[KeyMapping])
    kryo.register(classOf[CompactPatternList])
    kryo.register(classOf[CompactOnePatternList])
    kryo.register(classOf[CompactTwoPatternList])
    kryo.register(classOf[CompactArrayPatternList])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofInt])
    kryo.register(classOf[FilteringCondition])
  }
}


class LongMapSerializer extends Serializer[mutable.LongMap[Array[ValuePatternInstance]]] {
  override def read(kryo: Kryo, input: Input, type1: Class[mutable.LongMap[Array[ValuePatternInstance]]]): mutable.LongMap[Array[ValuePatternInstance]] = {


    val keys = kryo.readObject(input, classOf[Array[Long]])
    val values = kryo.readObject(input, classOf[Array[Array[ValuePatternInstance]]])

    mutable.LongMap.fromZip(keys, values)
  }

  override def write(kryo: Kryo, output: Output, object1: mutable.LongMap[Array[ValuePatternInstance]]): Unit = {

    kryo.writeObject(output, object1.keys.toArray)
    kryo.writeObject(output, object1.values.toArray)

  }
}

class LongMapCompactSerializer extends Serializer[mutable.LongMap[CompactPatternList]] {
  override def read(kryo: Kryo, input: Input, type1: Class[mutable.LongMap[CompactPatternList]]): mutable.LongMap[CompactPatternList] = {


    val keys = kryo.readObject(input, classOf[Array[Long]])
    val values = kryo.readObject(input, classOf[Array[CompactPatternList]])


    mutable.LongMap.fromZip(keys, values)
  }

  override def write(kryo: Kryo, output: Output, object1: mutable.LongMap[CompactPatternList]): Unit = {

    kryo.writeObject(output, object1.keys.toArray)
    kryo.writeObject(output, object1.values.toArray)

  }
}





