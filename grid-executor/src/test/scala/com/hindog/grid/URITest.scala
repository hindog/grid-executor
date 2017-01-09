package com.hindog.grid

import java.net.URI

import org.scalatest.FunSuite

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class URITest extends FunSuite {

  test("resolve file://foo/a/b") {
    assertResult(new URI("file:///foo/a/b"))(new URI("file:///foo/a") / "b")
    assertResult(new URI("file:///foo/a/b"))(new URI("file:///foo/a/") / "b")
  }

  test("resolve '..'") {
    assertResult(new URI("file:///foo/a/"))(new URI("file:///foo/a/file.jar") / "..")
    assertResult(new URI("file:///foo/a/"))(new URI("file:///foo/a/b/file.jar") / ".." / "..")
  }
}
