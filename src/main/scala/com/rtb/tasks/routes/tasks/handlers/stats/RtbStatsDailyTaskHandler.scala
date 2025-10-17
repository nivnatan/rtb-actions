package com.rtb.tasks.routes.tasks.handlers.stats

import akka.stream.scaladsl.{Sink, Source}
import com.common.clients.druid.{DruidSql, DruidStats, QueryResult}
import com.common.clients.kafka.KafkaProducerQueue
import com.common.clients.kafka.KafkaProducerQueue.{KafkaProducerConfig, KafkaProducerRequest}
import com.common.utils.json.JsonUtil
import com.common.utils.logging.LoggingSupport
import com.common.utils.types.TypesUtil._
import com.rtb.tasks.config.{Config, ConfigSupport}
import com.rtb.tasks.routes.tasks.handlers.TaskHandler
import com.rtb.tasks.routes.tasks.models.TaskRequest
import scala.concurrent.duration._

/**
  * Created by Niv on 18/12/2022
  */
final class RtbStatsDailyTaskHandler(val config: Config) extends TaskHandler with ConfigSupport with LoggingSupport {

  private val druid     = DruidSql(DruidStats.Url, Some(DruidStats.Username), Some(DruidStats.Password))
  private val publisher = new KafkaProducerQueue(KafkaProducerConfig("rtb-stats.rapidbidding.com:3032"))

  override def handle(taskRequest: TaskRequest): Unit = {
    val query = getQuery(taskRequest)
    debug(s"running RtbStatsDailyTaskHandler. query = $query")
    druid.runQuery(query)
      .toFutureEither
      .foreach { res =>
        debug(s"query result. query = ${res.map(_.result.length)}")
        res.map(onResponse(_, taskRequest))
      }
  }

  private def onResponse(queryResult: QueryResult, taskRequest: TaskRequest): Unit = {
    Source(queryResult.result)
      .throttle(5000, 1.seconds)
      .runWith(Sink.foreach {
        row =>
          //val data = row + ("datetime" -> s"${taskRequest.params("date")}T00:00:00.000Z")
          val data = row + ("datetime" -> row("__time"))
          JsonUtil.toJson(data).foreach { json =>
            publisher.publish(KafkaProducerRequest(json, "rtb_stats"))
            //fluentd.log(FluentdLoggerRequest("rtb_stats", data))
          }
      }).onComplete(_ => debug("done."))
  }

  private def getQuery(taskRequest: TaskRequest): String = {
    s"""|
        |select * from rtb_stats
        |where __time > (CURRENT_TIMESTAMP - INTERVAL '91' DAY)
        |and "date" = '${taskRequest.params("date")}'
        |and "hour" in (0,1,2,3,4,5,6,7,8,9)
        |and adm_cost < 10000000
        |""".stripMargin
  }

  private def getQueryAgg(taskRequest: TaskRequest): String = {
    s"""|select
        |"date",
        |ssp_id,
        |rule_id,
        |dsp_id,
        |dsp_endpoint_id,
        |rejection_type,
        |error_type,
        |dynamic_error_type,
        |beacon_type,
        |ssp_rtb_partner_id,
        |dsp_rtb_partner_id,
        |sum(bid_requests) as bid_requests,
        |sum(bid_responses) as bid_responses,
        |sum(bidder_requests) as bidder_requests,
        |sum(beacon_requests) as beacon_requests,
        |sum(errors_count) as errors_count,
        |sum(rejection_count) as rejection_count,
        |sum(response_time) as response_time,
        |sum(bid_sum) as bid_sum,
        |sum(dynamic_requests_sent) as dynamic_requests_sent,
        |sum(dynamic_timeouts) as dynamic_timeouts,
        |sum(dynamic_errors) as dynamic_errors,
        |sum(dynamic_response_time) as dynamic_response_time,
        |sum(dynamic_bids) as dynamic_bids,
        |sum(dynamic_bids_sum) as dynamic_bids_sum,
        |sum(adm_requests) as adm_requests,
        |sum(adm_cost) as adm_cost,
        |sum(nurl_requests) as nurl_requests,
        |sum(nurl_cost) as nurl_cost,
        |sum(burl_requests) as burl_requests,
        |sum(burl_cost) as burl_cost,
        |sum(cost) as cost,
        |sum(dynamic_revenue) as dynamic_revenue,
        |sum(revenue) as revenue,
        |sum(beacon_imps_count) as beacon_imps_count,
        |sum(beacon_views_count) as beacon_views_count,
        |sum(beacon_clicks_count) as beacon_clicks_count,
        |sum(beacon_convs_count) as beacon_convs_count,
        |sum(beacon_errors_count) as beacon_errors_count,
        |sum(dynamic_partner_revenue) as dynamic_partner_revenue,
        |sum(request_content_length) as request_content_length,
        |sum(dynamic_request_content_length) as dynamic_request_content_length,
        |sum(dynamic_imps_count) as dynamic_imps_count
        |from rtb_stats
        |where __time > (CURRENT_TIMESTAMP - INTERVAL '91' DAY)
        |and "date" = '${taskRequest.params("date")}'
        |and adm_cost < 10000000
        |group by
        |"date",
        |ssp_id,
        |rule_id,
        |dsp_id,
        |dsp_endpoint_id,
        |rejection_type,
        |error_type,
        |dynamic_error_type,
        |beacon_type,
        |ssp_rtb_partner_id,
        |dsp_rtb_partner_id
        |""".stripMargin
  }
}