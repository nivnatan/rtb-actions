package com.rtb.tasks.routes.tasks.handlers.stats2.p3.csv

import akka.stream.scaladsl.{Sink, Source}
import com.common.clients.druid.{DruidSql, DruidStats, QueryResult}
import com.common.rtb.models.openrtb.RtbImpressionTypes
import com.common.utils.logging.LoggingSupport
import com.rtb.tasks.config.{Config, ConfigSupport}
import com.rtb.tasks.routes.tasks.handlers.TaskHandler
import com.rtb.tasks.routes.tasks.models.TaskRequest
import com.rtb.tasks.utils.counters.Counters.{StatsAggregationV3TaskQueryFailureCount, StatsAggregationV3TaskQuerySuccessCount}
import java.io.{File, FileOutputStream, PrintWriter}
import java.nio.file.{Files, Paths}
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.concurrent.Future
import com.common.utils.types.TypesUtil._
import scala.concurrent.duration.DurationInt

/**
 * Created by Niv on 27/07/2025
 */
class RtbStatsP3CsvTaskHandler(val config: Config) extends TaskHandler with ConfigSupport with LoggingSupport {

  private val druid     = DruidSql(DruidStats.Url, Some(DruidStats.Username), Some(DruidStats.Password))

  override def handle(taskRequest: TaskRequest): Unit = {
    val query = getQuery(taskRequest)
    debug(s"running RtbStatsP3CsvTaskHandler. query = $query")
    runQuery(query)
      .toFutureEither
      .foreach { res =>
        debug(s"query result. query = ${res.map(_.result.length)}")
        res.map(onResponse(_, taskRequest))
      }
  }

  private def onResponse(queryResult: QueryResult, taskRequest: TaskRequest): Unit = {
    Source(queryResult.result)
      .runWith(Sink.seq)
      .map {
        rows =>
          try {
            saveToDb(rows, taskRequest)
          } catch  {
            case e: Exception =>
              error(s"error saving to db. $e")
          }
      }.onComplete(_ => debug("done."))
  }

  private def getQuery(taskRequest: TaskRequest): String = {
    val date = taskRequest.params("date")
    s"""|
        |select
        |"date",
        |"hour",
        |ssp_id,
        |dsp_id,
        |impression_types as s_request_format,
        |dsp_response_imp_type as d_response_format,
        |ssp_response_imp_type,
        |bid_requests,
        |bid_responses,
        |dynamic_requests as dsp_bid_requests,
        |dynamic_bids as dsp_bid_responses,
        |ssp_imps_count as ssp_imps,
        |dynamic_imps as impressions,
        |ssp_cost / 1000 as cost_without_fees,
        |cost / 1000 as cost,
        |dynamic_revenue / 1000 as gross_revenue,
        |dynamic_partner_revenue / 1000 as net_revenue
        |from(SELECT
        |"date",
        |"hour",
        |ssp_id,
        |dsp_id,
        |impression_types,
        |dsp_response_imp_type,
        |ssp_response_imp_type,
        |COALESCE(sum(bid_requests), 0) as bid_requests,
        |COALESCE(sum(bid_responses), 0) as bid_responses,
        |COALESCE(sum(dynamic_requests_sent), 0) as dynamic_requests,
        |COALESCE(sum(dynamic_bids), 0) as dynamic_bids,
        |COALESCE(sum(ssp_imps_count), 0) as ssp_imps_count,
        |COALESCE(sum(dynamic_imps_count), 0) as dynamic_imps,
        |COALESCE(sum(dynamic_revenue), 0) as dynamic_revenue,
        |COALESCE(sum(dynamic_partner_revenue), 0) as dynamic_partner_revenue,
        |COALESCE(sum(ssp_cost), 0) as ssp_cost,
        |COALESCE(sum(cost), 0) as cost
        |from rtb_stats
        |where __time > (CURRENT_TIMESTAMP - INTERVAL '45' DAY)
        |and "date" = '${date}'
        |and (ssp_rtb_partner_id = 3 and dsp_rtb_partner_id = 3)
        |group by
        |"date",
        |"hour",
        |ssp_id,
        |dsp_id,
        |impression_types,
        |dsp_response_imp_type,
        |ssp_response_imp_type
        |)
        |UNION ALL(
        |select
        |"date",
        |"hour",
        |ssp_id,
        |'0' as dsp_id,
        |impression_types as s_request_format,
        |dsp_response_imp_type as d_response_format,
        |ssp_response_imp_type,
        |bid_requests,
        |bid_responses,
        |dynamic_requests as dsp_bid_requests,
        |dynamic_bids as dsp_bid_responses,
        |ssp_imps_count as ssp_imps,
        |dynamic_imps as impressions,
        |ssp_cost / 1000 as cost_without_fees,
        |cost / 1000 as cost,
        |dynamic_revenue / 1000 as gross_revenue,
        |dynamic_partner_revenue / 1000 as net_revenue
        |from(SELECT
        |"date",
        |"hour",
        |ssp_id,
        |impression_types,
        |'' as dsp_response_imp_type,
        |ssp_response_imp_type,
        |COALESCE(sum(bid_requests), 0) as bid_requests,
        |COALESCE(sum(bid_responses), 0) as bid_responses,
        |0 as dynamic_requests,
        |0 as dynamic_bids,
        |COALESCE(sum(ssp_imps_count), 0) as ssp_imps_count,
        |0 as dynamic_imps,
        |0 as dynamic_revenue,
        |0 as dynamic_partner_revenue,
        |COALESCE(sum(ssp_cost), 0) as ssp_cost,
        |COALESCE(sum(cost), 0) as cost
        |from rtb_stats
        |where __time > (CURRENT_TIMESTAMP - INTERVAL '45' DAY)
        |and "date" = '${date}'
        |and (ssp_rtb_partner_id = 3 and dsp_rtb_partner_id != 3)
        |group by
        |"date",
        |"hour",
        |ssp_id,
        |impression_types,
        |ssp_response_imp_type
        |)
        |)
        |""".stripMargin
  }

  private def runQuery(query: String): Future[QueryResult] = {
    runQuery(druid, query)
  }

  private def runQuery(druid: DruidSql, query: String): Future[QueryResult] = {
    druid.runQuery(query).retry(druid.runQuery(query), 5.seconds, 3)(ec, system.scheduler).failTo(QueryResult.empty)
      .andOnComplete { r =>
        debug(s"query result for ${druid.druidUri}. length = ${r.map(_.result.length)}")
        countersHandler ! (if(r.isSuccess) StatsAggregationV3TaskQuerySuccessCount else StatsAggregationV3TaskQueryFailureCount)
      }
  }

  private def saveToDb(data: Seq[Map[String, String]], taskRequest: TaskRequest): Unit = {
    saveToCsv(data.map(initData(_, taskRequest)), taskRequest)
  }

  private def initData(row: Map[String, String], taskRequest: TaskRequest): Map[String, String] = {
    val sspContextData        = row.get("ssp_id").filter(_ != "").flatMap(dao.getRtbSspContextData)
    val dspData               = row.get("dsp_id").filter(_ != "").flatMap(dao.getRtbDspData)
    val dspBidderData         = dspData.map(_.bidderId).flatMap(dao.getRtbBidderData)
    val dspPartnerData        = dspData.map(_.partnerId).flatMap(dao.getRtbPartnerData)
    val dspSubPartnerData     = dspData.flatMap(_.subPartnerId).flatMap(dao.getRtbPartnerData)
    val sspRequestFormat      = row.get("s_request_format").map(_.split(",").flatMap(f => RtbImpressionTypes.getImpressionType(f).map(_.name))).map(_.mkString(","))
    val dspRequestFormat      = row.get("d_response_format").map(_.split(",").flatMap(f => RtbImpressionTypes.getImpressionType(f).map(_.name))).map(_.mkString(","))

    val additionalDataMap = Map(
      "dsp_id"              -> (if(dspPartnerData.exists(a => a.id == "3")) row.get("dsp_id") else Some("")),
      "s_request_format"    -> sspRequestFormat,
      "d_response_format"    -> dspRequestFormat
    ).collect { case (k, Some(s)) => k -> s }

    row ++ additionalDataMap
  }

  private def saveToCsv(data: Seq[Map[String, String]], taskRequest: TaskRequest): Unit = {
    writeCsv(data, s"/usr/share/data/stats/${taskRequest.params("date")}/stats_${taskRequest.params("date")}.csv")
  }

  def writeCsv(data: Seq[Map[String, String]], filePath: String): Unit = {

    // Ensure parent directories exist
    val parentDir = Paths.get(filePath).getParent
    if (parentDir != null) {
      Files.createDirectories(parentDir)
    }

    val columns = Seq(
      "date",
      "hour",
      "ssp_id",
      "s_request_format",
      "dsp_id",
      "d_response_format",
      "bid_responses",
      "bid_requests",
      "dsp_bid_requests",
      "dsp_bid_responses",
      "impressions",
      "cost",
      "cost_without_fees",
      "gross_revenue",
      "net_revenue")

    val csvFile = new File(filePath)
    val writer = new PrintWriter(filePath)

    val quoteColumns: Set[String] = Set("s_request_format")

    try {
      writer.println(columns.mkString(",")) // Write headers in defined order
      data.foreach { row =>
        val values = columns.map(col => row.getOrElse(col, ""))
        writer.println(
          columns.map { col =>
            val value = row.getOrElse(col, "")
            if (quoteColumns.contains(col)) s""""${value.replace("\"", "\"\"")}"""" else value
          }.mkString(","))
      }
    } finally {
      writer.close()
    }

    // Now create a zip file containing the CSV
    val zipFilePath = filePath.stripSuffix(".csv") + ".zip"
    val zipOut = new ZipOutputStream(new FileOutputStream(zipFilePath))

    try {
      val zipEntry = new ZipEntry(csvFile.getName)
      zipOut.putNextEntry(zipEntry)
      val bytes = Files.readAllBytes(csvFile.toPath)
      zipOut.write(bytes, 0, bytes.length)
      zipOut.closeEntry()
    } finally {
      zipOut.close()
    }

    // Optional: Delete the original CSV file to keep only the ZIP
    csvFile.delete()
  }
}