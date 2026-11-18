package com.rtb.admin.service.buckets

import com.common.clients.db.Database
import com.common.utils.http.HttpUtil
import com.rtb.admin.routes.actions.models.ActionRequest
import com.rtb.admin.service.buckets.BucketsDelete.DeleteOutcome
import scala.util.Try

/**
 * Created by Niv on 18/11/2026
 */
object BucketsDelete {
  case class DeleteOutcome(deletedCount: Int, totalBefore: Int, totalAfter: Int)
}

class BucketsDelete(rtbDb: Database, rtbDbDev: Database) {

  def delete(bucketId: Long, bucketValues: Set[String], isDev: Boolean = false): Try[DeleteOutcome] = {

    val db = if (isDev) rtbDbDev else rtbDb

    db.withTransaction { conn =>

      // Disable bucket-touch triggers for this session (bulk op)
      {
        val ps = conn.prepareStatement("SET @rtb_skip_bucket_touch = 1")
        try ps.execute() finally ps.close()
      }

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

      // Re-enable triggers for this session (optional but good hygiene)
      {
        val ps = conn.prepareStatement("SET @rtb_skip_bucket_touch = 0")
        try ps.execute() finally ps.close()
      }

      // Touch parent bucket ONCE so pollers reload (monotonic even within same second)
      {
        val ps = conn.prepareStatement(
          "UPDATE rtb_buckets " +
            "SET bucket_version = bucket_version + 1 " +
            "WHERE id = ?"
        )
        try {
          ps.setLong(1, bucketId)
          ps.executeUpdate()
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
