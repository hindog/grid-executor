package com.hindog.grid

import java.net.URI

import com.hindog.grid.repo.Resource
import org.scalatest.FunSuite

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class ResourceTest extends FunSuite {

  test("parse invalid resource") {
    assertResult(None)(Resource.parse(new URI("file:///foo/bar.jar")))
    assertResult(Some(Resource(new URI("file:///foo/"), "707350a2eeb1fa2ed77a32ddb3893ed308e941db/file.jar")))(Resource.parse(new URI("file:///foo/707350a2eeb1fa2ed77a32ddb3893ed308e941db/file.jar")))
    assertResult(Some(Resource(new URI("file:///foo/"), "707350a2eeb1fa2ed77a32ddb3893ed308e941db/file.jar")))(Resource.parse(new URI("file:///foo/707350a2eeb1fa2ed77a32ddb3893ed308e941db/file.jar")))
  }

}
