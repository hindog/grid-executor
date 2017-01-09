package com.hindog.grid.repo

import com.hindog.grid.Hook
import org.gridkit.vicluster.telecontrol.ClasspathUtils

import scala.collection.JavaConversions._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class SyncRepositoryHook(repo: Repository) extends Hook("sync-repository") {
  override def run(): Unit = {
    val cp = ClasspathUtils.listCurrentClasspath()

    try {
      cp.flatMap(u => Resource.parse(u.toURI)).foreach(r => {
        if (!repo.contains(r)) {
          repo.upload(r)
        }
      })
    } finally {
      repo.close
    }
  }
}
