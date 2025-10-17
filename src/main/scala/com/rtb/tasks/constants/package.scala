package com.rtb.tasks

import akka.http.scaladsl.server.Rejection
import scala.language.implicitConversions

/**
  * Created by Niv on 12/12/2021
  */
package object constants {

  object Tasks {
    sealed abstract class Task(val name: String)
    case object RtbStatsAggregationTask extends Task("stats_agg")
    case object RtbStatsAggregationV2Task extends Task("stats_agg_v2")
    case object RtbStatsAggregationP3Task extends Task("stats_agg_p3")
    case object RtbStatsDailyTask extends Task("stats_daily")
    case object BidSwitchApiTask extends Task("bidswitch_api")
    case object CurrenciesApiTask extends Task("currencies_api")
    case object RtbBucketsTask extends Task("rtb_buckets")
    case object RtbStatsAggregationV3Task extends Task("stats_agg_v3")
    case object RtbStatsExtendedTask extends Task("rtb_stats_extended")
    case object RtbStatsExtendedP3Task extends Task("rtb_stats_extended_p3")
    case object RtbStatsExtendedP3CsvTask extends Task("rtb_stats_extended_csv_p3")
    case object RtbStatsP3CsvTask extends Task("rtb_stats_csv_p3")
    case object RtbStatsMappingP3CsvTask extends Task("rtb_stats_mapping_csv_p3")
  }

  object Rejections {
    case class UnsupportedTask(name: String) extends Rejection
  }
}
