package com.rtb.admin.service.buckets

import com.common.clients.db.Database
import scala.util.Try

/**
 * Created by Niv on 18/11/2026
 */
class BucketsGet(rtbDb: Database, rtbDbDev: Database) {

  def get(bucketId: Long, isDev: Boolean = false): Try[Vector[String]] = {
    val db = if (isDev) rtbDbDev else rtbDb
    db.withTransaction { conn =>
      val ps = conn.prepareStatement(
        "SELECT bucket_value FROM rtb_bucket_values WHERE bucket_id = ? ORDER BY bucket_value"
      )
      try {
        ps.setLong(1, bucketId)
        val rs = ps.executeQuery()
        try {
          val values = Vector.newBuilder[String]
          while (rs.next()) values += rs.getString(1)
          values.result()
        } finally rs.close()
      } finally ps.close()
    }
  }
}
