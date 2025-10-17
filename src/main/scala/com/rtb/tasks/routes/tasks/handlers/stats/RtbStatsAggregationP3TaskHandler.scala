package com.rtb.tasks.routes.tasks.handlers.stats

import akka.stream.scaladsl.{Sink, Source}
import com.common.clients.druid._
import com.common.clients.fluentd.{FluentdLoggerRequest, FluentdLoggerRequests}
import com.common.utils.counters.RtbStatsExtendedCounters.{RtbStatsExtendedP3RequestFailureCount, RtbStatsExtendedP3RequestSuccessCount}
import com.common.utils.listeners.CompletionListener
import com.common.utils.logging.LoggingSupport
import com.common.utils.time.MyLocalDateTime
import com.common.utils.types.TypesUtil._
import com.rtb.tasks.config.{Config, ConfigSupport}
import com.rtb.tasks.routes.tasks.handlers.TaskHandler
import com.rtb.tasks.routes.tasks.models.TaskRequest
import com.rtb.tasks.utils.counters.Counters.{StatsAggregationV2TaskQueryFailureCount, StatsAggregationV2TaskQuerySuccessCount}
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Created by Niv on 20/05/2023
  */
final class RtbStatsAggregationP3TaskHandler(val config: Config) extends TaskHandler with ConfigSupport with LoggingSupport {

  private val druidUs       = DruidSql(DruidBeaconsUs.Url, Some(DruidBeaconsUs.Username), Some(DruidBeaconsUs.Password))
  private val druidUs2      = DruidSql(DruidBeaconsUs2.Url, Some(DruidBeaconsUs2.Username), Some(DruidBeaconsUs2.Password))
  private val druidUs2_2    = DruidSql(DruidBeaconsUs2_2.Url, Some(DruidBeaconsUs2_2.Username), Some(DruidBeaconsUs2_2.Password))
  private val druidUs3      = DruidSql(DruidBeaconsUs3.Url, Some(DruidBeaconsUs3.Username), Some(DruidBeaconsUs3.Password))
  private val druidEu       = DruidSql(DruidBeaconsEu.Url, Some(DruidBeaconsEu.Username), Some(DruidBeaconsEu.Password))
  private val druidEu_2     = DruidSql(DruidBeaconsEu_2.Url, Some(DruidBeaconsEu_2.Username), Some(DruidBeaconsEu_2.Password))
  private val druidAs       = DruidSql(DruidBeaconsAs.Url, Some(DruidBeaconsAs.Username), Some(DruidBeaconsAs.Password))
  private val druidAs_2       = DruidSql(DruidBeaconsAs_2.Url, Some(DruidBeaconsAs_2.Username), Some(DruidBeaconsAs_2.Password))
  private val druidUs_2     = DruidSql(DruidBeaconsUs_2.Url, Some(DruidBeaconsUs_2.Username), Some(DruidBeaconsUs_2.Password))
  private val druidUs3_2    = DruidSql(DruidBeaconsUs3_2.Url, Some(DruidBeaconsUs3_2.Username), Some(DruidBeaconsUs3_2.Password))
  private val druidUsw      = DruidSql(DruidBeaconsUsw.Url, Some(DruidBeaconsUsw.Username), Some(DruidBeaconsUsw.Password))
  private val druidUsw_2    = DruidSql(DruidBeaconsUsw_2.Url, Some(DruidBeaconsUsw_2.Username), Some(DruidBeaconsUsw_2.Password))
  private val druidUsw_3    = DruidSql(DruidBeaconsUsw_3.Url, Some(DruidBeaconsUsw_3.Username), Some(DruidBeaconsUsw_3.Password))

  private val listener      = CompletionListener(RtbStatsExtendedP3RequestSuccessCount, RtbStatsExtendedP3RequestFailureCount, countersHandler)

  override def handle(taskRequest: TaskRequest): Unit = {
    val query = getQuery(taskRequest)
    debug(s"running RtbStatsAggregationP3Task. query = $query")
    runQuery(query)
      .toFutureEither
      .foreach { res =>
        debug(s"query result. query = ${res.map(_.result.length)}")
        res.map(onResponse(_, taskRequest))
      }
  }

  private def onResponse(queryResult: QueryResult, taskRequest: TaskRequest): Unit = {
    Source(queryResult.result)
      .throttle(5000, 5.seconds)
      .grouped(5000)
      .runWith(Sink.foreach {
        rows =>
          try {
            val data = initData(rows)
            saveToDb(data, taskRequest)
          } catch  {
            case e: Exception =>
              error(s"error saving to db. $e")
          }
      }).onComplete(_ => debug("done."))
  }

  private def getQuery(taskRequest: TaskRequest): String = {
    val date = taskRequest.params("date")
    val fromHour = taskRequest.params.get("fromHour").map(h => s"$h:00:00").getOrElse("00:00:00")
    val toHour = taskRequest.params.get("toHour").map(h => if(h == "00") "23:59:59.9999" else s"$h:00:00").getOrElse("23:59:59.9999")
    s"""|
        |SELECT * from(select '$date' as "date", ssp_id, dsp_endpoint_id, country, app_bundle, size, impression_types, endpoint_imp_type as dsp_endpoint_imp_type, dc_id, supply_publisher_id, bid_requests, bid_responses, dynamic_requests_sent, dynamic_bids, adm_requests, nurl_requests, burl_requests, beacon_views_count, dynamic_imps, (adm_requests * 1.0 / (bid_responses + 1) * 1.0) * 100 as wins_rate, (cost * 1.0 / 1000) as cost, (cost * 1.0 / revenue) * 100 as cost_perc, (dynamic_revenue * 1.0 / 1000) as dynamic_revenue, (dynamic_partner_revenue * 1.0 / 1000) as dynamic_partner_revenue, (revenue * 1.0 / 1000) as revenue, (((revenue - cost) * 1.0) / 1000) as net, (dynamic_bids * 1.0 / (dynamic_requests_sent + 1) * 1.0) * 100 as bid_rate, (dynamic_revenue * 1.0 / (dynamic_requests_sent + 1) * 1.0) as rpm, ((revenue - cost)  * 1.0 / (dynamic_requests_sent + 1) * 1.0) as npm from (
        |  select ssp_id, dsp_endpoint_id, country, app_bundle, size, impression_types, endpoint_imp_type, dc_id, supply_publisher_id,
        |  sum(CASE WHEN beacon_type = '10' then "count" else 0 end) as bid_requests,
        |  sum(CASE WHEN beacon_type = '11' then "count" else 0 end) as bid_responses,
        |  sum(CASE WHEN beacon_type = '12' then "count" else 0 end) as dynamic_requests_sent,
        |  sum(CASE WHEN beacon_type = '13' then "count" else 0 end) as dynamic_bids,
        |  sum(CASE WHEN beacon_type = '5' then "count" else 0 end) as adm_requests,
        |  sum(CASE WHEN beacon_type = '6' then "count" else 0 end) as nurl_requests,
        |  sum(CASE WHEN beacon_type = '7' then "count" else 0 end) as burl_requests,
        |  sum(CASE WHEN beacon_type = '2' then "count" else 0 end) as beacon_views_count,
        |  sum(dynamic_imps) as dynamic_imps,
        |  sum(cost) as cost,
        |  sum(dynamic_rev) as dynamic_revenue,
        |  sum(dynamic_partner_revenue) as dynamic_partner_revenue,
        |  sum(revenue) as revenue
        |  from rtb_beacons_agg
        |  WHERE TIMESTAMP '$date $fromHour' <= "__time" AND "__time" < TIMESTAMP '$date $toHour'
        |  and (ssp_rtb_partner_id = 3 or dsp_rtb_partner_id = 3)
        |  group by ssp_id, dsp_endpoint_id, country, app_bundle, size, impression_types, endpoint_imp_type, dc_id, supply_publisher_id
        |))
        |where ((cost > 0 or dynamic_requests_sent > 10000 or dynamic_revenue > 0 or adm_requests > 1) or bid_requests > 10000)
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
        countersHandler ! (if(r.isSuccess) StatsAggregationV2TaskQuerySuccessCount else StatsAggregationV2TaskQueryFailureCount)
      }
  }

  private def initData(rows: Seq[Map[String, String]]): Seq[Map[String, String]] = {
    rows
      .map(row => row + ("datetime" -> MyLocalDateTime.now.dateTimeUtc))
      .map(row => if(row.contains("dsp_endpoint_id")) row + ("dsp_id" -> dao.getRtbDspEndpointData(row("dsp_endpoint_id")).map(_.dspId).getOrElse("")) else row)
      .map(row => if(row.contains("ssp_id")) row + ("ssp_rtb_partner_id" -> dao.getRtbSspEndpointData(row("ssp_id")).map(_.sspId).flatMap(dao.getRtbSspData).map(_.partnerId).getOrElse("")) else row)
      .map(row => if(row.contains("dsp_id")) row + ("dsp_rtb_partner_id" -> dao.getRtbDspData(row("dsp_id")).map(_.partnerId).getOrElse("")) else row)
      .map(row => if(row.contains("ssp_id")) row + ("ssp_title" -> dao.getRtbSspEndpointData(row("ssp_id")).map(_.title).getOrElse("")) else row)
      .map(row => if(row.contains("dsp_id")) row + ("dsp_title" -> dao.getRtbDspData(row("dsp_id")).map(_.title).getOrElse("")) else row)
  }

  private def saveToDb(data: Seq[Map[String, String]], taskRequest: TaskRequest): Unit = {
    fluentd.log(FluentdLoggerRequests(data.map(row => FluentdLoggerRequest("rtb_stats_extended_p3", row, Some(listener)))))
  }
}