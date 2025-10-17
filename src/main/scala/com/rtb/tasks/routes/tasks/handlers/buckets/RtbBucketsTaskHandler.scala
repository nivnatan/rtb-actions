package com.rtb.tasks.routes.tasks.handlers.buckets

import com.common.rtb.models.data.RtbBucketValuesData
import com.common.utils.http.HttpUtil
import com.common.utils.logging.LoggingSupport
import com.rtb.tasks.config.{Config, ConfigSupport}
import com.rtb.tasks.routes.tasks.handlers.TaskHandler
import com.rtb.tasks.routes.tasks.models.TaskRequest
import scala.collection.immutable.Seq

/**
  * Created by Niv on 28/08/2023
  */
final class RtbBucketsTaskHandler(val config: Config) extends TaskHandler with ConfigSupport with LoggingSupport {

  override def handle(taskRequest: TaskRequest): Unit = {
    taskRequest.params.get("bucket_id").flatMap(dao.getRtbBucketValues).fold(throw new IllegalArgumentException(s"bucket ${taskRequest.params.get("bucket_id")} is no defined."))(handle(_, taskRequest))
  }

  private def handle(bucketValuesData: RtbBucketValuesData, taskRequest: TaskRequest): Unit = {
    val newBucketValues         =
      taskRequest.payload
        .replace("\r\n", "\n")
        .replace("\r", "\n")
        .split("\n")
        .map(_.trim)
        .map(_.toLowerCase)
        .toSet

    if(newBucketValues.nonEmpty) handle(newBucketValues, bucketValuesData, taskRequest)
  }

  private def handle(newBucketValues: Set[String], bucketValuesData: RtbBucketValuesData, taskRequest: TaskRequest): Unit = {
    val deleteSql = s"""|DELETE FROM
                        |rtb_bucket_values
                        |WHERE bucket_id = ${bucketValuesData.id}
                        |""".stripMargin

    val insertSql = s"""|INSERT INTO rtb_bucket_values
                        | (bucket_id, bucket_value) VALUES ${newBucketValues.map { line => val decoded = HttpUtil.urlDecodeValue(line); val r = s"""\"$decoded\""""; s"(${bucketValuesData.id},$r)" }.mkString(",")}
                        |""".stripMargin

    val result = rtbDb.executeTransaction(Seq(deleteSql, insertSql))

    debug(s"oldBucketValuesSize=${bucketValuesData.bucketValues.size}, newBucketValuesSize=${newBucketValues.size}. result=$result")(taskRequest.debug)
  }
}
