package org.apache.spark.disc.util.misc

import org.apache.log4j.Logger

trait LogAble {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
}
