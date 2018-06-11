package com.hindog.grid.repo

import com.hindog.grid.{Hook, Logging}
import org.gridkit.vicluster.telecontrol.ClasspathUtils

import scala.collection.JavaConversions._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class SyncRepositoryHook(repo: Repository) extends Hook("sync-repository") with Logging {
  override def run(): Unit = {
    val cp = ClasspathUtils.listCurrentClasspath()

    try {
      cp.flatMap(u => Resource.parse(u.toURI)).foreach(r => {
        logger.debug(s"Repo contains: ${r.path}? ${repo.contains(r)}")
        if (!repo.contains(r)) {
          logger.debug(s"Uploading: ${r.path}...")
          repo.upload(r)
        }
      })
    } finally {
      repo.close
    }
  }
}
