package com.hindog.grid

import scala.collection.mutable


trait Configurable {

  protected[grid] val config: mutable.ListBuffer[Config => Config] = mutable.ListBuffer[Config => Config]()

  def conf(f: Config => Config): Unit = config += f

}
