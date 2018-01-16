package org.apache.spark.Logo.UnderLying.dataStructure

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.serializer.{KryoRegistrator, KryoSerializer}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class KryoRegistor extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[mutable.LongMap[ArrayBuffer[ValuePatternInstance]]], new LongMapSerializer())
  }
}



class LongMapSerializer extends Serializer[mutable.LongMap[ArrayBuffer[ValuePatternInstance]]]{
  override def read(kryo: Kryo, input: Input, type1: Class[mutable.LongMap[ArrayBuffer[ValuePatternInstance]]]): mutable.LongMap[ArrayBuffer[ValuePatternInstance]] = {


    val keys = kryo.readObject(input,classOf[Array[Long]])
    val values = kryo.readObject(input,classOf[Array[ArrayBuffer[ValuePatternInstance]]])

    mutable.LongMap.fromZip(keys,values)
  }

  override def write(kryo: Kryo, output: Output, object1: mutable.LongMap[ArrayBuffer[ValuePatternInstance]]): Unit = {
    kryo.writeObject(output,object1.keys.toArray)
    kryo.writeObject(output,object1.values.toArray)

  }
}



