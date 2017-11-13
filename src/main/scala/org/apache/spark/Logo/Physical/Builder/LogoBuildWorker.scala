package org.apache.spark.Logo.Physical.Builder

import scala.reflect.ClassTag

class LogoBuildWorker(logoBuildScriptStep: LogoBuildScriptStep) {

}

class LogoOneOnOneBuildWorker[A:ClassTag, B:ClassTag, C:ClassTag](logoBuildScriptOneOnOne: LogoBuildScriptOneOnOne[A,B,C]) extends LogoBuildWorker(logoBuildScriptOneOnOne){

}

class LogoMultiOnOneBuildWorker[A:ClassTag, B:ClassTag, C:ClassTag, D:ClassTag](logoBuildScripMultiOnOne: LogoBuildScripMultiOnOne[A,B,C,D]) extends LogoBuildWorker(logoBuildScripMultiOnOne){

}

