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
    _replaceDelta(bucketId, bucketValues, adminRequest) match {
      case Success(values) =>
        countersHandler ! RtbActionsBucketReplaceRequestsSuccessCount
        val oldValues = values.map(v => "\"" + v + "\"").mkString("[", ",", "]")
        val response =
          s"""{
             |"bucketId": $bucketId,
             |"oldValuesCount": ${values.length},
             |"newValuesCount": ${bucketValues.size},
             |"oldValues": $oldValues
             |}""".stripMargin
        ActionSuccess(response)

      case Failure(e) =>
        countersHandler ! RtbActionsBucketReplaceRequestsFailureCount
        SqlBucketError(e.getMessage)
    }
  }

  private def _replaceDelta(bucketId: Long, bucketValues: Set[String], adminRequest: ActionRequest): Try[Vector[String]] = {
    val BatchSize = 5000
    val db = if (adminRequest.isDev) rtbDbDev else rtbDb

    db.withTransaction { conn =>
      // 1) Snapshot current
      val oldValues: Vector[String] = {
        val ps = conn.prepareStatement(
          "SELECT bucket_value FROM rtb_bucket_values WHERE bucket_id=?"
        )
        try {
          ps.setLong(1, bucketId)
          val rs  = ps.executeQuery()
          val buf = Vector.newBuilder[String]
          while (rs.next()) buf += rs.getString(1)
          rs.close()
          buf.result()
        } finally ps.close()
      }

      // 2) Delta sets
      val newSet = bucketValues.iterator.map(HttpUtil.urlDecodeValue).toSet
      val oldSet = oldValues.toSet
      val toInsert = newSet -- oldSet
      val toDelete = oldSet -- newSet

      debug(s"ReplaceHandler. toInsertSize=${toInsert.size}, toDeleteSize=${toDelete.size}")

      // 3) Delete gone (chunked)
      if (toDelete.nonEmpty) {
        val del = conn.prepareStatement(
          "DELETE FROM rtb_bucket_values WHERE bucket_id=? AND bucket_value=?"
        )
        try {
          var n = 0
          toDelete.foreach { v =>
            del.setLong(1, bucketId)
            del.setString(2, v)
            del.addBatch()
            n += 1
            if (n % BatchSize == 0) del.executeBatch()
          }
          del.executeBatch()
        } finally del.close()
      }

      // 4) Insert new (chunked)
      if (toInsert.nonEmpty) {
        val ins = conn.prepareStatement(
          "INSERT INTO rtb_bucket_values (bucket_id, bucket_value) VALUES (?,?)"
        )
        try {
          var n = 0
          toInsert.foreach { v =>
            ins.setLong(1, bucketId)
            ins.setString(2, v)
            ins.addBatch()
            n += 1
            if (n % BatchSize == 0) ins.executeBatch()
          }
          ins.executeBatch()
        } finally ins.close()
      }

      // final expression = result
      oldValues
    }
  }

  private def _replace(bucketId: Long, bucketValues: Set[String], adminRequest: ActionRequest): Try[Vector[String]] = {
    val db = if(adminRequest.isDev) rtbDbDev else rtbDb
    db.withTransaction { conn =>
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
