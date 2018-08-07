package com.hindog.grid.repo

class ResourceNotAcceptedException(val resource: Resource, val repository: Repository, reason: String) extends RuntimeException(s"Resource $resource was rejected by repository $repository (reason: $reason)")
object ResourceNotAcceptedException {
  def readOnly(resource: Resource, repo: Repository): ResourceNotAcceptedException = new ResourceNotAcceptedException(resource, repo, s"Repository '$repo' is read-only")
}

class ResourceNotFoundException(val filename: String, val contentHash: String, val repository: Repository) extends RuntimeException(s"No resource matching filename '$filename' and content hash '$contentHash' was not found in repository $repository")

class RepositoryException(val message: String, val cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
}
