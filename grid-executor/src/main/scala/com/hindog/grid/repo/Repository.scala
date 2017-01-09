package com.hindog.grid.repo

import java.net.URI


/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
trait Repository extends Serializable {
  def uri: URI
  def contains(resource: Resource): Boolean
  def upload(resource: Resource): Resource
  def resolve(resource: Resource): Resource
  def close()
}

