package org.apache.spark.disc.util.misc

import org.apache.log4j.Logger

trait LogAble {
//  org.apache.log4j.PropertyConfigurator
//    .configure("./src/resources/log4j.preperties")

  lazy val logger: Logger = Logger.getLogger(this.getClass)
}
