package com.rtb.tasks.routes.tasks.handlers.stats2.p3.csv

import akka.stream.scaladsl.{Sink, Source}
import com.common.clients.druid._
import com.common.utils.logging.LoggingSupport
import com.common.utils.types.TypesUtil._
import com.rtb.tasks.config.{Config, ConfigSupport}
import com.rtb.tasks.routes.tasks.handlers.TaskHandler
import com.rtb.tasks.routes.tasks.models.TaskRequest
import com.rtb.tasks.utils.counters.Counters.{StatsAggregationV3TaskQueryFailureCount, StatsAggregationV3TaskQuerySuccessCount}

import java.io.{File, FileOutputStream, PrintWriter}
import java.nio.file.{Files, Paths}
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Created by Niv on 13/07/2025
 */
class RtbStatsExtendedP3CsvTaskHandler(val config: Config) extends TaskHandler with ConfigSupport with LoggingSupport {

  private val druidUs         = DruidSql(DruidBeaconsUs.Url, Some(DruidBeaconsUs.Username), Some(DruidBeaconsUs.Password))
  private val druidUs2        = DruidSql(DruidBeaconsUs2.Url, Some(DruidBeaconsUs2.Username), Some(DruidBeaconsUs2.Password))
  private val druidUs2_2      = DruidSql(DruidBeaconsUs2_2.Url, Some(DruidBeaconsUs2_2.Username), Some(DruidBeaconsUs2_2.Password))
  private val druidUs3        = DruidSql(DruidBeaconsUs3.Url, Some(DruidBeaconsUs3.Username), Some(DruidBeaconsUs3.Password))
  private val druidEu         = DruidSql(DruidBeaconsEu.Url, Some(DruidBeaconsEu.Username), Some(DruidBeaconsEu.Password))
  private val druidEu_2       = DruidSql(DruidBeaconsEu_2.Url, Some(DruidBeaconsEu_2.Username), Some(DruidBeaconsEu_2.Password))
  private val druidAs         = DruidSql(DruidBeaconsAs.Url, Some(DruidBeaconsAs.Username), Some(DruidBeaconsAs.Password))
  private val druidAs_2       = DruidSql(DruidBeaconsAs_2.Url, Some(DruidBeaconsAs_2.Username), Some(DruidBeaconsAs_2.Password))
  private val druidUs_2       = DruidSql(DruidBeaconsUs_2.Url, Some(DruidBeaconsUs_2.Username), Some(DruidBeaconsUs_2.Password))
  private val druidUs3_2      = DruidSql(DruidBeaconsUs3_2.Url, Some(DruidBeaconsUs3_2.Username), Some(DruidBeaconsUs3_2.Password))
  private val druidUsw        = DruidSql(DruidBeaconsUsw.Url, Some(DruidBeaconsUsw.Username), Some(DruidBeaconsUsw.Password))
  private val druidUsw_2      = DruidSql(DruidBeaconsUsw_2.Url, Some(DruidBeaconsUsw_2.Username), Some(DruidBeaconsUsw_2.Password))
  private val druidUsw_3      = DruidSql(DruidBeaconsUsw_3.Url, Some(DruidBeaconsUsw_3.Username), Some(DruidBeaconsUsw_3.Password))

  override def handle(taskRequest: TaskRequest): Unit = {
    val query = getQuery(taskRequest)
    debug(s"running RtbStatsExtendedP3CsvTaskHandler. query = $query")
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
        |SELECT * from(select '$date' as "date", ssp_id, dsp_id, country, app_bundle, size, impression_types, dsp_response_imp_type, placement_id, num_of_schain_nodes, ssp_response_imp_type, bid_requests, bid_responses, dsp_bid_requests, dsp_bid_responses, ssp_imps_count, dsp_imps_count, (cost * 1.0 / 1000) as cost, (ssp_cost * 1.0 / 1000) as ssp_cost, (revenue * 1.0 / 1000) as revenue, (partner_revenue * 1.0 / 1000) as partner_revenue from (
        |  select ssp_id, dsp_id, country, app_bundle, main_size_orig as size, impression_types, dsp_response_imp_type, placement_id, num_of_schain_nodes, ssp_response_imp_type,
        |  COALESCE(sum(CASE WHEN beacon_type = '10' then "count" else 0 end), 0) as bid_requests,
        |  COALESCE(sum(CASE WHEN beacon_type = '11' then "count" else 0 end), 0) as bid_responses,
        |  COALESCE(sum(CASE WHEN beacon_type = '12' and dsp_rtb_partner_id = 3 then "count" else 0 end), 0) as dsp_bid_requests,
        |  COALESCE(sum(CASE WHEN beacon_type = '13' and dsp_rtb_partner_id = 3 then "count" else 0 end), 0) as dsp_bid_responses,
        |  COALESCE(sum(cost * "count"), 0) as cost,
        |  COALESCE(sum(ssp_cost * "count"), 0) as ssp_cost,
        |  COALESCE(sum(case when dsp_rtb_partner_id = 3 then dynamic_rev * "count" else 0 end), 0) as revenue,
        |  COALESCE(sum(case when dsp_rtb_partner_id = 3 then dynamic_partner_revenue * "count" else 0 end), 0) as partner_revenue,
        |  COALESCE(sum(ssp_imps * "count"), 0) as ssp_imps_count,
        |  COALESCE(sum(dynamic_imps * "count"), 0) as dsp_imps_count
        |  from rtb_beacons
        |  WHERE TIME_FORMAT(__time, 'YYYY-MM-dd') = '$date'
        |  and (ssp_rtb_partner_id = 3 and dsp_rtb_partner_id = 3)
        |  group by ssp_id, dsp_id, country, app_bundle, main_size_orig, impression_types, dsp_response_imp_type, placement_id, num_of_schain_nodes, ssp_response_imp_type
        |  limit 3
        |))
        |where (bid_responses > 0 or bid_requests > 0 or dsp_bid_requests > 0 or dsp_bid_responses > 0 or ssp_imps_count > 0 or dsp_imps_count > 0 or cost > 0 or ssp_cost > 0 or revenue > 0 or partner_revenue > 0)
        |UNION ALL(
        |SELECT * from(select '$date' as "date", ssp_id, '' as dsp_id, country, app_bundle, size, impression_types, '' as dsp_response_imp_type, '' as placement_id, num_of_schain_nodes, ssp_response_imp_type, bid_requests, bid_responses, dsp_bid_requests, dsp_bid_responses, ssp_imps_count, dsp_imps_count, (cost * 1.0 / 1000) as cost, (ssp_cost * 1.0 / 1000) as ssp_cost, (revenue * 1.0 / 1000) as revenue, (partner_revenue * 1.0 / 1000) as partner_revenue from (
        |  select ssp_id, country, app_bundle, main_size_orig as size, impression_types, num_of_schain_nodes, ssp_response_imp_type,
        |  COALESCE(sum(CASE WHEN beacon_type = '10' then "count" else 0 end), 0) as bid_requests,
        |  COALESCE(sum(CASE WHEN beacon_type = '11' then "count" else 0 end), 0) as bid_responses,
        |  0 as dsp_bid_requests,
        |  0 as dsp_bid_responses,
        |  COALESCE(sum(cost * "count"), 0) as cost,
        |  COALESCE(sum(ssp_cost * "count"), 0) as ssp_cost,
        |  0 as revenue,
        |  0 as partner_revenue,
        |  COALESCE(sum(ssp_imps * "count"), 0) as ssp_imps_count,
        |  0 as dsp_imps_count
        |  from rtb_beacons
        |  WHERE TIME_FORMAT(__time, 'YYYY-MM-dd') = '$date'
        |  and (ssp_rtb_partner_id = 3 and dsp_rtb_partner_id != 3)
        |  group by ssp_id, country, app_bundle, main_size_orig, impression_types, num_of_schain_nodes, ssp_response_imp_type
        |  limit 3
        |))
        |where (bid_responses > 0 or bid_requests > 0 or dsp_bid_requests > 0 or dsp_bid_responses > 0 or ssp_imps_count > 0 or dsp_imps_count > 0 or cost > 0 or ssp_cost > 0 or revenue > 0 or partner_revenue > 0)
        |)
        |""".stripMargin
  }

  private def runQuery(query: String): Future[QueryResult] = {
    val us      = runQuery(druidUs, query)
    val us2     = runQuery(druidUs2, query)
    val us2_2   = runQuery(druidUs2_2, query)
    val us3     = runQuery(druidUs3, query)
    val eu      = runQuery(druidEu, query)
    val eu_2    = runQuery(druidEu_2, query)
    val as      = runQuery(druidAs, query)
    val as_2    = runQuery(druidAs_2, query)
    val us_2    = runQuery(druidUs_2, query)
    val us3_2   = runQuery(druidUs3_2, query)
    val usw     = runQuery(druidUsw, query)
    val usw_2   = runQuery(druidUsw_2, query)
    val usw_3   = runQuery(druidUsw_3, query)
    Future.sequence(Seq(us, us2, us2_2, us3, eu, eu_2, as, as_2, us_2, us3_2, usw, usw_2, usw_3)).map(_.reduceLeft(_ ++ _))
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
    val sspContextData    = row.get("ssp_id").filter(_ != "").flatMap(dao.getRtbSspContextData)
    val dspData           = row.get("dsp_id").filter(_ != "").flatMap(dao.getRtbDspData)
    val dspBidderData     = dspData.map(_.bidderId).flatMap(dao.getRtbBidderData)
    val dspPartnerData    = dspData.map(_.partnerId).flatMap(dao.getRtbPartnerData)
    val dspSubPartnerData = dspData.flatMap(_.subPartnerId).flatMap(dao.getRtbPartnerData)
    val additionalDataMap = Map(
      "dsp_id" -> (if(dspPartnerData.exists(a => a.id == "3")) row.get("dsp_id") else Some("0"))
    ).collect { case (k, Some(s)) if s != "" => k -> s }

    row ++ additionalDataMap
  }

  private def saveToCsv(data: Seq[Map[String, String]], taskRequest: TaskRequest): Unit = {
    writeCsv(data, s"/usr/share/data/${taskRequest.params("date")}/stats_${taskRequest.params("date")}.csv")
  }

  def writeCsv(data: Seq[Map[String, String]], filePath: String): Unit = {

    // Ensure parent directories exist
    val parentDir = Paths.get(filePath).getParent
    if (parentDir != null) {
      Files.createDirectories(parentDir)
    }

    val columns = Seq(
      "date",
      "ssp_id",
      "country",
      "app_bundle",
      "size",
      "impression_types",
      "num_of_schain_nodes",
      "dsp_id",
      "placement_id",
      "dsp_response_imp_type",
      "ssp_response_imp_type",
      "bid_responses",
      "bid_requests",
      "dsp_bid_requests",
      "dsp_bid_responses",
      "ssp_imps_count",
      "cost",
      "ssp_cost",
      "revenue",
      "partner_revenue")

    val csvFile = new File(filePath)
    val writer = new PrintWriter(filePath)

    try {
      writer.println(columns.mkString(",")) // Write headers in defined order
      data.foreach { row =>
        val values = columns.map(col => row.getOrElse(col, ""))
        writer.println(values.mkString(","))
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

