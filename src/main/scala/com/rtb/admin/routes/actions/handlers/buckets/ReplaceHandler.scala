package com.rtb.admin.routes.actions.handlers.buckets

import com.common.utils.http.HttpUtil
import com.common.utils.logging.LoggingSupport
import com.rtb.admin.config.{Config, ConfigSupport}
import com.rtb.admin.routes.actions.models.{ActionRequest, ActionResult, ActionSuccess}
import com.common.utils.types.TypesUtil._
import com.rtb.admin.routes.actions.constants.ActionErrors.BucketsErrorTypes.{InvalidBucketParameters, SqlBucketError}
import com.rtb.admin.routes.actions.handlers.ActionHandler
import com.rtb.admin.utils.counters.Counters.{RtbActionsBucketReplaceRequestsFailureCount, RtbActionsBucketReplaceRequestsSuccessCount}
import scala.util.{Failure, Success, Try}

/**
 * Created by Niv on 17/10/2025
 */
class ReplaceHandler(val config: Config) extends ActionHandler with ConfigSupport with LoggingSupport {

  def handle(adminRequest: ActionRequest): ActionResult = {
    (for {
      bucketId     <- adminRequest.params.get("bucket_id").flatMap(_.toLongSafe)
      bucketValues = adminRequest.payload.replace("\r\n", "\n").replace("\r", "\n").split("\n").map(_.trim).map(_.toLowerCase).toSet
      if (bucketValues.nonEmpty)
    } yield replace(bucketId, bucketValues, adminRequest))
      .getOrElse(InvalidBucketParameters)
  }

  private def replace(bucketId: Long, bucketValues: Set[String], adminRequest: ActionRequest): ActionResult = {
    _replace(bucketId, bucketValues, adminRequest) match {
      case Success(values) =>
        countersHandler ! RtbActionsBucketReplaceRequestsSuccessCount
        //val oldValues = values.map(v => "\"" + v + "\"").mkString("[", ",", "]")
        val oldValuesCount = values
        val response =
          s"""{
             |"bucketId": $bucketId,
             |"oldValuesCount": $oldValuesCount,
             |"newValuesCount": ${bucketValues.size},
             |}""".stripMargin
        ActionSuccess(response)

      case Failure(e) =>
        countersHandler ! RtbActionsBucketReplaceRequestsFailureCount
        SqlBucketError(e.getMessage)
    }
  }

  private def _replace(bucketId: Long, bucketValues: Set[String], adminRequest: ActionRequest): Try[Int] = {
    val db = if(adminRequest.isDev) rtbDbDev else rtbDb
    db.withTransaction { conn =>
      // 1) Snapshot existing values
      val oldCount = {
        val ps = conn.prepareStatement(
          "SELECT COUNT(*) FROM rtb_bucket_values WHERE bucket_id = ?"
        )
        try {
          ps.setLong(1, bucketId)
          val rs = ps.executeQuery()
          rs.next()
          val count = rs.getInt(1)
          rs.close()
          count
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

      // 3) single INSERT with many placeholders (replaces the batch)
      if (bucketValues.nonEmpty) {
        val values = bucketValues.iterator.map(HttpUtil.urlDecodeValue).toVector
        val placeholders = values.map(_ => "(?, ?)").mkString(",")
        val sql = s"INSERT INTO rtb_bucket_values (bucket_id, bucket_value) VALUES $placeholders"

        val ps = conn.prepareStatement(sql)
        try {
          var i = 1
          values.foreach { v =>
            ps.setLong(i, bucketId); i += 1
            ps.setString(i, v);      i += 1
          }
          ps.executeUpdate() // one round-trip
        } finally ps.close()
      }

      oldCount
    }
  }
}
