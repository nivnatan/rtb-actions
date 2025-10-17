package com.rtb.actions.routes.handlers.buckets

import com.common.utils.http.HttpUtil
import com.rtb.actions.config.{Config, ConfigSupport}
import com.rtb.actions.routes.actions.models.{ActionRequest, ActionResult, ActionSuccess}
import com.common.utils.types.TypesUtil._
import com.rtb.actions.constants.ActionErrors.BucketsErrorTypes.{InvalidBucketParameters, SqlBucketError}
import com.rtb.actions.routes.handlers.ActionHandler
import scala.util.{Failure, Success, Try}

/**
 * Created by Niv on 17/10/2025
 */
class ReplaceHandler(val config: Config) extends ActionHandler with ConfigSupport {

  def handle(adminRequest: ActionRequest): ActionResult = {
    (for {
      bucketId     <- adminRequest.params.get("bucket_id").flatMap(_.toLongSafe)
      bucketValues = adminRequest.payload.replace("\r\n", "\n").replace("\r", "\n").split("\n").map(_.trim).map(_.toLowerCase).toSet
      if (bucketValues.nonEmpty)
    } yield replace(bucketId, bucketValues))
      .getOrElse(InvalidBucketParameters)
  }

  private def replace(bucketId: Long, bucketValues: Set[String]): ActionResult = {
    _replace(bucketId, bucketValues) match {
      case Success(values) =>
        val oldValues = values.map(v => "\"" + v + "\"").mkString("[", ",", "]")
        val response = s"""{"bucketId":"$bucketId", "oldValues":"$oldValues"}"""
        ActionSuccess(response)

      case Failure(e) =>
        SqlBucketError(e.getMessage)
    }
  }

  private def _replace(bucketId: Long, bucketValues: Set[String]): Try[Vector[String]] = {
    rtbDb.withTransaction { conn =>
      // 1) Snapshot existing values
      val oldValues = {
        val ps = conn.prepareStatement(
          "SELECT bucket_value FROM rtb_bucket_values WHERE bucket_id = ?"
        )
        try {
          ps.setLong(1, bucketId)
          val rs = ps.executeQuery()
          val buf = Vector.newBuilder[String]
          while (rs.next()) buf += rs.getString(1)
          rs.close()
          buf.result()
        } finally ps.close()
      }

      // 2) Delete all for bucket
      {
        val ps = conn.prepareStatement(
          "DELETE FROM rtb_bucket_values WHERE bucket_id = ?"
        )
        try {
          ps.setLong(1, bucketId)
          ps.executeUpdate()
        } finally ps.close()
      }

      // 3) Insert new set (batch)
      val inserted = if (bucketValues.isEmpty) 0
      else {
        val ps = conn.prepareStatement(
          "INSERT INTO rtb_bucket_values (bucket_id, bucket_value) VALUES (?, ?)"
        )
        try {
          bucketValues.foreach { v0 =>
            val v = HttpUtil.urlDecodeValue(v0)
            ps.setLong(1, bucketId)
            ps.setString(2, v)
            ps.addBatch()
          }
          ps.executeBatch().sum
        } finally ps.close()
      }

      oldValues
    }
  }
}
