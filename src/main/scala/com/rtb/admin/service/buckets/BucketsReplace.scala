package com.rtb.admin.service.buckets

import com.common.clients.db.Database
import com.common.utils.http.HttpUtil
import scala.util.Try

/**
 * Created by Niv on 18/11/2026
 */
class BucketsReplace(rtbDb: Database, rtbDbDev: Database) {

  def replace(bucketId: Long, bucketValues: Set[String], isDev: Boolean = false): Try[Int] = {
    val db = if (isDev) rtbDbDev else rtbDb
    db.withTransaction { conn =>

      // Disable bucket-touch triggers for this session (bulk op)
      {
        val ps = conn.prepareStatement("SET @rtb_skip_bucket_touch = 1")
        try ps.execute() finally ps.close()
      }


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
            ps.setLong(i, bucketId);
            i += 1
            ps.setString(i, v);
            i += 1
          }
          ps.executeUpdate() // one round-trip
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

      oldCount
    }
  }
}
