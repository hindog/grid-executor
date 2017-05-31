package com.hindog

import java.net.URI

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
package object grid {

  implicit class AnyExtensions[A](a: A) {
    def ifThen[B >: A](expr: A => Boolean)(f: A => B): B = if (expr(a)) f(a) else a
    def ifThen[B >: A](expr: => Boolean)(f: A => B): B = if (expr) f(a) else a
    def ifThenElse[B >: A](expr: => Boolean)(f: A => B)(e: A => B): B = if (expr) f(a) else e(a)
    def ifThenElse[B >: A](expr: A => Boolean)(f: A => B)(e: A => B): B = if (expr(a)) f(a) else e(a)
    def ifDefinedThen[B >: A, C](o: Option[C])(f: (A, C) => B): B = if (o.isDefined) f(a, o.get) else a
  }

  implicit class URIExtensions(uri: URI) {
    def /(that: String) : URI = if (uri.getPath.endsWith("/")) {
      uri.resolve(that).normalize()
    } else {
      uri.resolve(uri.getPath + "/" + that).normalize()
    }
  }
}
