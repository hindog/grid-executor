package com.hindog.grid

import java.net.{URL, URLClassLoader}

import scala.collection._

/**
  * Created by atom-aaron on 6/3/17
  */
class PostDelegatingURLClassLoader(urls: Array[URL], parent: ClassLoader) extends ClassLoader {
  val cl = new URLClassLoader(urls)

  override def loadClass(name: String): Class[_] = {
    var loaded = cl.findLoadedClass(name)

    loaded = if (loaded != null) {
      try {
        cl.loadClass(name)
      } catch {
        case ex: ClassNotFoundException => null
      }
    } else loaded

    loaded = if (loaded == null) parent.loadClass(name) else loaded

    loaded
  }
}
