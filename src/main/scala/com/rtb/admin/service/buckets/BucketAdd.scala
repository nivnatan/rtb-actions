package com.rtb.admin.service.buckets

import com.common.clients.db.Database
import com.common.utils.http.HttpUtil
import com.rtb.admin.service.buckets.BucketAdd.AddOutcome

import scala.util.Try

/**
 * Created by Niv on 18/11/2026
 */
object BucketAdd {
  case class AddOutcome(addedCount: Int, totalCount: Int, addedValues: Vector[String])
}

class BucketAdd(rtbDb: Database, rtbDbDev: Database) {

  def add(bucketId: Long, bucketValues: Set[String], isDev: Boolean = false): Try[AddOutcome] = {
    val db = if(isDev) rtbDbDev else rtbDb
    db.withTransaction { conn =>

      // Disable bucket-touch triggers for this session (bulk op)
      {
        val ps = conn.prepareStatement("SET @rtb_skip_bucket_touch = 1")
        try ps.execute() finally ps.close()
      }

      val psInsert = conn.prepareStatement(
        "INSERT IGNORE INTO rtb_bucket_values (bucket_id, bucket_value) VALUES (?, ?)"
      )
      try {
        val values = bucketValues.toVector.map(HttpUtil.urlDecodeValue)
        values.foreach { v =>
          psInsert.setLong(1, bucketId)
          psInsert.setString(2, v)
          psInsert.addBatch()
        }
        val results = psInsert.executeBatch()
        val insertedCount = results.count(_ > 0)

        val psCount = conn.prepareStatement(
          "SELECT COUNT(*) FROM rtb_bucket_values WHERE bucket_id = ?"
        )
        val totalCount =
          try {
            psCount.setLong(1, bucketId)
            val rs = psCount.executeQuery()
            rs.next()
            val count = rs.getInt(1)
            rs.close()
            count
          } finally psCount.close()

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

        AddOutcome(insertedCount, totalCount, values)
      } finally psInsert.close()
    }
  }
}
