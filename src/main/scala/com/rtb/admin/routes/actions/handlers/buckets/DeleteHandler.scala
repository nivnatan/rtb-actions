package com.rtb.admin.routes.actions.handlers.buckets

import com.common.utils.http.HttpUtil
import com.common.utils.logging.LoggingSupport
import com.rtb.admin.config.{Config, ConfigSupport}
import com.rtb.admin.routes.actions.constants.ActionErrors.BucketsErrorTypes.{InvalidBucketParameters, SqlBucketError}
import com.rtb.admin.routes.actions.handlers.ActionHandler
import com.rtb.admin.routes.actions.models.{ActionRequest, ActionResult, ActionSuccess}
import scala.util.{Failure, Success, Try}
import com.common.utils.types.TypesUtil._
import com.rtb.admin.utils.counters.Counters.{RtbActionsBucketDeleteRequestsFailureCount, RtbActionsBucketDeleteRequestsSuccessCount}

/**
 * Created by Niv on 15/12/2025
 */
class DeleteHandler(val config: Config) extends ActionHandler with ConfigSupport with LoggingSupport {

  private case class DeleteOutcome(deletedCount: Int, totalBefore: Int, totalAfter: Int)

  def handle(adminRequest: ActionRequest): ActionResult = {
    (for {
      bucketId     <- adminRequest.params.get("bucket_id").flatMap(_.toLongSafe)
      bucketValues = adminRequest.payload.replace("\r\n", "\n").replace("\r", "\n").split("\n").map(_.trim).map(_.toLowerCase).toSet
      if (bucketValues.nonEmpty)
    } yield delete(bucketId, bucketValues, adminRequest))
      .getOrElse(InvalidBucketParameters)
  }

  private def delete(bucketId: Long, bucketValues: Set[String], adminRequest: ActionRequest): ActionResult = {
    _delete(bucketId, bucketValues, adminRequest) match {
      case Success(values) =>
        countersHandler ! RtbActionsBucketDeleteRequestsSuccessCount
        //val oldValues = values.map(v => "\"" + v + "\"").mkString("[", ",", "]")
        val oldValuesCount = values.totalBefore
        val newValuesCount = values.totalAfter
        val deletedCount   = values.deletedCount
        val response =
          s"""{
             |"bucketId": $bucketId,
             |"oldValuesCount": $oldValuesCount,
             |"newValuesCount": $newValuesCount,
             |"deletedCount": $deletedCount,
             |}""".stripMargin
        ActionSuccess(response)

      case Failure(e) =>
        countersHandler ! RtbActionsBucketDeleteRequestsFailureCount
        SqlBucketError(e.getMessage)
    }
  }

  private def _delete(
                       bucketId: Long,
                       bucketValues: Set[String],
                       adminRequest: ActionRequest
                     ): Try[DeleteOutcome] = {

    val db = if (adminRequest.isDev) rtbDbDev else rtbDb

    db.withTransaction { conn =>

      // 1) Count before
      val totalBefore: Int = {
        val ps = conn.prepareStatement(
          "SELECT COUNT(*) FROM rtb_bucket_values WHERE bucket_id = ?"
        )
        try {
          ps.setLong(1, bucketId)
          val rs = ps.executeQuery()
          rs.next()
          rs.getInt(1)
        } finally ps.close()
      }

      // 2) Delete requested values (single statement)
      val deletedCount: Int =
        if (bucketValues.isEmpty) 0
        else {
          val values = bucketValues.iterator.map(HttpUtil.urlDecodeValue).toVector
          val placeholders = values.map(_ => "?").mkString(",")

          val sql =
            s"""
               |DELETE FROM rtb_bucket_values
               |WHERE bucket_id = ?
               |AND bucket_value IN ($placeholders)
               |""".stripMargin

          val ps = conn.prepareStatement(sql)
          try {
            ps.setLong(1, bucketId)
            var i = 2
            values.foreach { v =>
              ps.setString(i, v)
              i += 1
            }
            ps.executeUpdate() // rows actually deleted
          } finally ps.close()
        }

      // 3) Compute after (cheap, no extra query)
      val totalAfter = totalBefore - deletedCount

      DeleteOutcome(
        deletedCount = deletedCount,
        totalBefore  = totalBefore,
        totalAfter   = totalAfter
      )
    }
  }
}