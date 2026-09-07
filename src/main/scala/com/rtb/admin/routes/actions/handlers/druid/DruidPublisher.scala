package com.rtb.admin.routes.actions.handlers.druid

import com.common.clients.kafka.KafkaProducerQueue
import com.common.clients.kafka.KafkaProducerQueue.{KafkaProducerConfig, KafkaProducerRequest}
import com.common.utils.json.JsonUtil
import com.common.utils.listeners.CompletionListener
import com.common.utils.time.MyLocalDateTime
import com.rtb.admin.config.{Config, ConfigSupport}
import com.rtb.admin.routes.actions.constants.ActionErrors.BucketsErrorTypes.InvalidBucketParameters
import com.rtb.admin.routes.actions.handlers.ActionHandler
import com.rtb.admin.routes.actions.models.{ActionRequest, ActionResult, ActionSuccess}
import com.rtb.admin.utils.counters.Counters.{RtbActionsDruidPublisherFailureCount, RtbActionsDruidPublisherSuccessCount}

/**
 * Created by Niv on 17/10/2025
 */
case class DruidRequest(hosts: String, topic: String, data: List[Map[String, Any]])
case class DruidRequestRaw(hosts: String, topic: String, data: String)

class DruidPublisher(val config: Config) extends ActionHandler with ConfigSupport {

  private val listenerDruid   = CompletionListener(RtbActionsDruidPublisherSuccessCount, RtbActionsDruidPublisherFailureCount, countersHandler)

  override def handle(actionRequest: ActionRequest): ActionResult = {
    (for{
      raw           <- JsonUtil.fromJson[DruidRequestRaw](actionRequest.payload)
      records       <- JsonUtil.fromJson[List[Map[String, Any]]](raw.data)
      druidRequest  = DruidRequest(raw.hosts, raw.topic, records)
    } yield save(druidRequest, actionRequest))
      .getOrElse(InvalidBucketParameters)
  }

  private def save(druidRequest: DruidRequest, actionRequest: ActionRequest): ActionResult = {
    val publisher     = new KafkaProducerQueue(KafkaProducerConfig(druidRequest.hosts))
    val data          = druidRequest.data.map(enrich)

    data.foreach(JsonUtil.toJson(_).foreach { json =>
      publisher.publish(KafkaProducerRequest(json, druidRequest.topic, Some(listenerDruid)))
    })

    ActionSuccess(data.length.toString)
  }

  private def enrich(dataMap: Map[String, Any]): Map[String, Any] = {
    val sspContextData          = dataMap.get("ssp_id").map(_.toString).flatMap(dao.getRtbSspContextData)
    val sspAdditionalDataMap    = Map(
      "datetime"              -> Some(MyLocalDateTime.now.dateTimeUtc),
      "rtb_ssp_id"            -> sspContextData.map(_.sspData.id),
      "ssp_rtb_partner_id"    -> sspContextData.map(_.partnerData.id),
      "ssp_bidder_id"         -> sspContextData.map(_.bidderData.id),
      "ssp_sub_partner_id"    -> sspContextData.flatMap(_.subPartnerData.map(_.id)),
    ).collect { case (k, Some(s)) if s != "" => k -> s }

    val dspContextData          =  dataMap.get("dsp_id").map(_.toString).flatMap(dao.getRtbDspContextData)
    val dspAdditionalDataMap    = Map(
      "dsp_id"                -> dspContextData.map(_.dspData.id),
      "dsp_rtb_partner_id"    -> dspContextData.map(_.partnerData.id),
      "dsp_bidder_id"         -> dspContextData.map(_.bidderData.id),
      "dsp_sub_partner_id"    -> dspContextData.flatMap(_.subPartnerData.map(_.id)),
    ).collect { case (k, Some(s)) if s != "" => k -> s }

    dataMap ++ sspAdditionalDataMap ++ dspAdditionalDataMap
  }
}
