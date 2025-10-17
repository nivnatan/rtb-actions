package com.rtb.actions.routes.handlers.buckets

import com.common.utils.http.HttpUtil
import com.rtb.actions.config.{Config, ConfigSupport}
import com.rtb.actions.constants.ActionErrors.BucketsErrorTypes.{InvalidBucketParameters, SqlBucketError}
import com.rtb.actions.routes.actions.models.{ActionRequest, ActionResult, ActionSuccess}
import com.rtb.actions.routes.handlers.ActionHandler
import scala.util.{Failure, Success, Try}
import com.common.utils.types.TypesUtil._

/**
 * Created by Niv on 17/10/2025
 */
class AddHandler(val config: Config) extends ActionHandler with ConfigSupport {

  private case class AddOutcome(addedCount: Int, totalCount: Int, addedValues: Vector[String])

  def handle(adminRequest: ActionRequest): ActionResult = {
    (for {
      bucketId     <- adminRequest.params.get("bucket_id").flatMap(_.toLongSafe)
      bucketValues = adminRequest.payload.replace("\r\n", "\n").replace("\r", "\n").split("\n").map(_.trim).map(_.toLowerCase).toSet
      if (bucketValues.nonEmpty)
    } yield replace(bucketId, bucketValues))
      .getOrElse(InvalidBucketParameters)
  }

  private def replace(bucketId: Long, bucketValues: Set[String]): ActionResult = {
    _add(bucketId, bucketValues) match {
      case Success(AddOutcome(addedCount, totalCount, addedValues)) =>
        val addedJson = addedValues.map(v => "\"" + v + "\"").mkString("[", ",", "]")
        val response =
          s"""{
             |"bucketId": $bucketId,
             |"addedCount": $addedCount,
             |"totalCount": $totalCount,
             |"addedValues": $addedJson
             |}""".stripMargin
        ActionSuccess(response)

      case Failure(e) =>
        SqlBucketError(e.getMessage)
    }
  }


  private def _add(bucketId: Long, bucketValues: Set[String]): Try[AddOutcome] = {
    rtbDb.withTransaction { conn =>
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

        // ✅ Fetch total count after insert
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

        AddOutcome(insertedCount, totalCount, values)
      } finally psInsert.close()
    }
  }
}