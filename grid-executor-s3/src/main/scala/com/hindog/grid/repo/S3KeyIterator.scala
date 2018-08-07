package com.hindog.grid.repo

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model._

import scala.collection._

/**
  * Copyright (c) Atom Tickets, LLC
  * Created: 6/21/18
  */
object S3KeyIterator {
  def apply(client: AmazonS3, request: ListObjectsRequest): Iterator[S3ObjectSummary] = new Iterator[S3ObjectSummary] {
    private var current = client.listObjects(request)
    private var itr = current.getObjectSummaries.iterator()

    override def hasNext: Boolean = {
      if (itr.hasNext) true else {
        if (current.isTruncated) {
          current = client.listObjects(request.withMarker(current.getNextMarker))
          itr = current.getObjectSummaries.iterator()
          itr.hasNext
        } else false
      }
    }

    override def next(): S3ObjectSummary = itr.next()
  }

  def apply(client: AmazonS3, builder: ListObjectsRequest => ListObjectsRequest): Iterator[S3ObjectSummary] = apply(client, builder(new ListObjectsRequest()))
  def apply(client: AmazonS3, bucketName: String, prefix: String): Iterator[S3ObjectSummary] = apply(client,_.withBucketName(bucketName).withMaxKeys(1000).withPrefix(prefix))
  def apply(client: AmazonS3, bucketName: String, prefixes: List[String]): Iterator[S3ObjectSummary] = {
    prefixes.iterator.foldLeft(Seq.empty[S3ObjectSummary].iterator)((acc, cur) => acc ++ apply(client, _.withBucketName(bucketName).withPrefix(cur).withMaxKeys(1000)))
  }

  def apply(client: AmazonS3, bucketName: String): Iterator[S3ObjectSummary] = apply(client, _.withBucketName(bucketName).withMaxKeys(1000))
}
