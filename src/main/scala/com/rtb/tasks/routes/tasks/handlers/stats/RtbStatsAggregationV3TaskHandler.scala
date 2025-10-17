package com.rtb.tasks.routes.tasks.handlers.stats

import akka.stream.scaladsl.{Sink, Source}
import com.common.clients.druid._
import com.common.clients.fluentd.FluentdLoggerRequest
import com.common.clients.kafka.KafkaProducerQueue
import com.common.clients.kafka.KafkaProducerQueue.{KafkaProducerConfig, KafkaProducerRequest}
import com.common.utils.counters.RtbStatsExtendedCounters._
import com.common.utils.json.JsonUtil
import com.common.utils.listeners.CompletionListener
import com.common.utils.logging.LoggingSupport
import com.common.utils.time.MyLocalDateTime
import com.rtb.tasks.config.{Config, ConfigSupport}
import com.rtb.tasks.routes.tasks.handlers.TaskHandler
import com.rtb.tasks.routes.tasks.models.TaskRequest
import com.rtb.tasks.utils.counters.Counters.{StatsAggregationV3TaskQueryFailureCount, StatsAggregationV3TaskQuerySuccessCount}
import scala.concurrent.Future
import com.common.utils.types.TypesUtil._
import scala.concurrent.duration._

/**
  * Created by Niv on 20/02/2024
  */
final class RtbStatsAggregationV3TaskHandler(val config: Config) extends TaskHandler with ConfigSupport with LoggingSupport {

  private val publisher     = new KafkaProducerQueue(KafkaProducerConfig("rtb-stats-dp-us.rapidbidding.com:3032"))

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

  private val listenerDruid   = CompletionListener(RtbStatsExtendedV3DruidRequestSuccessCount, RtbStatsExtendedV3DruidRequestFailureCount, countersHandler)
  private val listenerBq      = CompletionListener(RtbStatsExtendedV3BqRequestSuccessCount, RtbStatsExtendedV3BqRequestFailureCount, countersHandler)

  override def handle(taskRequest: TaskRequest): Unit = {
    val query = getQuery(taskRequest)
    debug(s"running RtbStatsAggregationV3TaskHandler. query = $query")
    runQuery(query)
      .toFutureEither
      .foreach { res =>
        debug(s"query result. query = ${res.map(_.result.length)}")
        res.map(onResponse(_, taskRequest))
      }
  }

  private def onResponse(queryResult: QueryResult, taskRequest: TaskRequest): Unit = {
    Source(queryResult.result)
      .throttle(20000, 5.seconds)
      .grouped(20000)
      .runWith(Sink.foreach {
        rows =>
          try {
            saveToDb(rows, taskRequest)
          } catch  {
            case e: Exception =>
              error(s"error saving to db. $e")
          }
      }).onComplete(_ => debug("done."))
  }

  private def getQuery(taskRequest: TaskRequest): String = {
    val time = MyLocalDateTime.hoursBefore()
    val date = taskRequest.params.getOrElse("date", time.dateTime)
    val hour = taskRequest.params.get("hour").map(_.toInt).getOrElse(time.hour)
    s"""|
        |SELECT * from(select '$date' as "date", bid_date, ssp_id, dsp_endpoint_id, country, app_bundle, size, impression_types, endpoint_imp_type as dsp_endpoint_imp_type, dc_id, supply_site_id, supply_publisher_id, dsp_cid, dsp_crid, dsp_adomain, dsp_seat, dsp_buyer_id, bid_requests, bid_responses, dynamic_requests_sent, dynamic_bids, adm_requests, nurl_requests, burl_requests, beacon_views_count, ssp_imps_count, dynamic_imps_count, (cost * 1.0 / 1000) as cost, (ssp_cost * 1.0 / 1000) as ssp_cost, (dynamic_revenue * 1.0 / 1000) as dynamic_revenue, (dynamic_partner_revenue * 1.0 / 1000) as dynamic_partner_revenue, (revenue * 1.0 / 1000) as revenue from (
        |  select bid_date, ssp_id, dsp_endpoint_id, country, app_bundle, size, impression_types, endpoint_imp_type, dc_id, supply_site_id, supply_publisher_id, dsp_cid, dsp_crid, dsp_adomain, dsp_seat, dsp_buyer_id,
        |  sum(CASE WHEN beacon_type = '10' then "count" else 0 end) as bid_requests,
        |  sum(CASE WHEN beacon_type = '11' then "count" else 0 end) as bid_responses,
        |  sum(CASE WHEN beacon_type = '12' then "count" else 0 end) as dynamic_requests_sent,
        |  sum(CASE WHEN beacon_type = '13' then "count" else 0 end) as dynamic_bids,
        |  sum(CASE WHEN beacon_type = '5' and beacon_count = 1 then "count" else 0 end) as adm_requests,
        |  sum(CASE WHEN beacon_type = '6' and beacon_count = 1 then "count" else 0 end) as nurl_requests,
        |  sum(CASE WHEN beacon_type = '7' and beacon_count = 1 then "count" else 0 end) as burl_requests,
        |  sum(CASE WHEN beacon_type = '2' and beacon_count = 1 then "count" else 0 end) as beacon_views_count,
        |  sum(cost * "count") as cost,
        |  sum(ssp_cost * "count") as ssp_cost,
        |  sum(dynamic_rev * "count") as dynamic_revenue,
        |  sum(dynamic_partner_revenue * "count") as dynamic_partner_revenue,
        |  sum(revenue * "count") as revenue,
        |  sum(ssp_imps * "count") as ssp_imps_count,
        |  sum(dynamic_imps * "count") as dynamic_imps_count
        |  from rtb_beacons
        |  WHERE TIME_FORMAT(__time, 'YYYY-MM-dd') = '$date'
        |  and TIME_EXTRACT(__time, 'HOUR') = $hour
        |  and ssp_id < 50
        |  group by bid_date, ssp_id, dsp_endpoint_id, country, app_bundle, size, impression_types, endpoint_imp_type, dc_id, supply_site_id, supply_publisher_id, dsp_cid, dsp_crid, dsp_adomain, dsp_seat, dsp_buyer_id
        |))
        |where ((cost > 0 or dynamic_requests_sent > 10000 or dynamic_revenue > 0 or dynamic_imps_count > 0 or adm_requests > 0 or burl_requests > 0) or bid_requests > 10000)
        |UNION ALL(
        |SELECT * from(select '$date' as "date", bid_date, ssp_id, dsp_endpoint_id, country, app_bundle, size, impression_types, endpoint_imp_type as dsp_endpoint_imp_type, dc_id, supply_site_id, supply_publisher_id, dsp_cid, dsp_crid, dsp_adomain, dsp_seat, dsp_buyer_id, bid_requests, bid_responses, dynamic_requests_sent, dynamic_bids, adm_requests, nurl_requests, burl_requests, beacon_views_count, ssp_imps_count, dynamic_imps_count, (cost * 1.0 / 1000) as cost, (ssp_cost * 1.0 / 1000) as ssp_cost, (dynamic_revenue * 1.0 / 1000) as dynamic_revenue, (dynamic_partner_revenue * 1.0 / 1000) as dynamic_partner_revenue, (revenue * 1.0 / 1000) as revenue from (
        |  select bid_date, ssp_id, dsp_endpoint_id, country, app_bundle, size, impression_types, endpoint_imp_type, dc_id, supply_site_id, supply_publisher_id, dsp_cid, dsp_crid, dsp_adomain, dsp_seat, dsp_buyer_id,
        |  sum(CASE WHEN beacon_type = '10' then "count" else 0 end) as bid_requests,
        |  sum(CASE WHEN beacon_type = '11' then "count" else 0 end) as bid_responses,
        |  sum(CASE WHEN beacon_type = '12' then "count" else 0 end) as dynamic_requests_sent,
        |  sum(CASE WHEN beacon_type = '13' then "count" else 0 end) as dynamic_bids,
        |  sum(CASE WHEN beacon_type = '5' and beacon_count = 1 then "count" else 0 end) as adm_requests,
        |  sum(CASE WHEN beacon_type = '6' and beacon_count = 1 then "count" else 0 end) as nurl_requests,
        |  sum(CASE WHEN beacon_type = '7' and beacon_count = 1 then "count" else 0 end) as burl_requests,
        |  sum(CASE WHEN beacon_type = '2' and beacon_count = 1 then "count" else 0 end) as beacon_views_count,
        |  sum(cost * "count") as cost,
        |  sum(ssp_cost * "count") as ssp_cost,
        |  sum(dynamic_rev * "count") as dynamic_revenue,
        |  sum(dynamic_partner_revenue * "count") as dynamic_partner_revenue,
        |  sum(revenue * "count") as revenue,
        |  sum(ssp_imps * "count") as ssp_imps_count,
        |  sum(dynamic_imps * "count") as dynamic_imps_count
        |  from rtb_beacons
        |  WHERE TIME_FORMAT(__time, 'YYYY-MM-dd') = '$date'
        |  and TIME_EXTRACT(__time, 'HOUR') = $hour
        |  and ssp_id >= 50
        |  group by bid_date, ssp_id, dsp_endpoint_id, country, app_bundle, size, impression_types, endpoint_imp_type, dc_id, supply_site_id, supply_publisher_id, dsp_cid, dsp_crid, dsp_adomain, dsp_seat, dsp_buyer_id
        |))
        |where ((cost > 0 or dynamic_requests_sent > 10000 or dynamic_revenue > 0 or dynamic_imps_count > 0 or adm_requests > 0 or burl_requests > 0) or bid_requests > 10000)
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
    data.
      map(initData(_, taskRequest))
      .foreach(saveToDb)
  }

  private def saveToDb(data: Map[String, String]): Unit = {
    saveToDruid(data)
    saveToBigQuery(data)
  }

  private def initData(row: Map[String, String], taskRequest: TaskRequest): Map[String, String] = {
    val sspContextData = row.get("ssp_id").filter(_ != "").flatMap(dao.getRtbSspContextData)
    val dspContextData = row.get("dsp_endpoint_id").filter(_ != "").flatMap(dao.getRtbDspContextData)
    val additionalDataMap = Map(
      "datetime"              -> Some(MyLocalDateTime.now.dateTimeUtc),
      "hour"                  -> Some(taskRequest.params.getOrElse("hour", MyLocalDateTime.hoursBefore().hour.toString)),
      "rtb_ssp_id"            -> sspContextData.map(_.sspData.id),
      "ssp_rtb_partner_id"    -> sspContextData.map(_.partnerData.id),
      "ssp_bidder_id"         -> sspContextData.map(_.bidderData.id),
      "ssp_sub_partner_id"    -> sspContextData.flatMap(_.subPartnerData.map(_.id)),
      "dsp_id"                -> dspContextData.map(_.dspData.id),
      "dsp_rtb_partner_id"    -> dspContextData.map(_.partnerData.id),
      "dsp_bidder_id"         -> dspContextData.map(_.bidderData.id),
      "dsp_sub_partner_id"        -> dspContextData.flatMap(_.subPartnerData.map(_.id))
    ).collect { case (k, Some(s)) if s != "" => k -> s }

    row ++ additionalDataMap
  }

  private def saveToDruid(data: Map[String, String]): Unit = {
    JsonUtil.toJson(data).foreach { json =>
      publisher.publish(KafkaProducerRequest(json, "rtb_stats_extended", Some(listenerDruid)))
    }
  }

  private def saveToBigQuery(data: Map[String, String]): Unit = {
    if(data.get("ssp_rtb_partner_id").contains("3") || data.get("dsp_rtb_partner_id").contains("3")) {
      fluentd.log(FluentdLoggerRequest("rtb_stats_extended_v2_p3", data, Some(listenerBq)))
    }
  }
}