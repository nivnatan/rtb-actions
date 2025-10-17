package com.rtb.tasks.routes.tasks.handlers

import akka.http.scaladsl.server.Directive1
import com.rtb.tasks.config.Config
import akka.http.scaladsl.server.Directives._
import com.rtb.tasks.constants.Rejections.UnsupportedTask
import com.rtb.tasks.constants.Tasks._
import com.rtb.tasks.routes.tasks.handlers.bidswitch.BidSwitchApiTaskHandler
import com.rtb.tasks.routes.tasks.handlers.buckets.RtbBucketsTaskHandler
import com.rtb.tasks.routes.tasks.handlers.currencies.CurrenciesApiTaskHandler
import com.rtb.tasks.routes.tasks.handlers.stats._
import com.rtb.tasks.routes.tasks.handlers.stats2.RtbStatsExtendedTaskHandler
import com.rtb.tasks.routes.tasks.handlers.stats2.p3.RtbStatsExtendedP3TaskHandler
import com.rtb.tasks.routes.tasks.handlers.stats2.p3.csv.{RtbStatsExtendedP3CsvTaskHandler, RtbStatsMappingP3CsvTaskHandler, RtbStatsP3CsvTaskHandler}
import com.rtb.tasks.routes.tasks.models.TaskRequest

/**
  * Created by Niv on 29/04/2022
  */
trait TaskHandler {
  def handle(taskRequest: TaskRequest): Unit
}

class TaskHandlers(config: Config) {

  private val statsAggTaskHandler               = new RtbStatsAggregationTaskHandler(config)
  private val statsAggV2TaskHandler             = new RtbStatsAggregationV2TaskHandler(config)
  private val statsAggP3TaskHandler             = new RtbStatsAggregationP3TaskHandler(config)
  private val statsDailyTaskHandler             = new RtbStatsDailyTaskHandler(config)
  private val bidSwitchApiTaskHandler           = new BidSwitchApiTaskHandler(config)
  private val currenciesApiTaskHandler          = new CurrenciesApiTaskHandler(config)
  private val rtbBucketsTaskHandler             = new RtbBucketsTaskHandler(config)
  private val statsAggV3TaskHandler             = new RtbStatsAggregationV3TaskHandler(config)
  private val statsExtendedTaskHandler          = new RtbStatsExtendedTaskHandler(config)
  private val statsExtendedP3TaskHandler        = new RtbStatsExtendedP3TaskHandler(config)
  private val statsExtendedP3CsvTaskHandler     = new RtbStatsExtendedP3CsvTaskHandler(config)
  private val statsP3CsvTaskHandler             = new RtbStatsP3CsvTaskHandler(config)
  private val statsMappingP3CsvTaskHandler      = new RtbStatsMappingP3CsvTaskHandler(config)

  def getHandler(name: String): Directive1[TaskHandler] = {
    name.toLowerCase match {
      case RtbStatsAggregationTask.name         => provide(statsAggTaskHandler)
      case RtbStatsAggregationV2Task.name       => provide(statsAggV2TaskHandler)
      case RtbStatsAggregationP3Task.name       => provide(statsAggP3TaskHandler)
      case BidSwitchApiTask.name                => provide(bidSwitchApiTaskHandler)
      case CurrenciesApiTask.name               => provide(currenciesApiTaskHandler)
      case RtbBucketsTask.name                  => provide(rtbBucketsTaskHandler)
      case RtbStatsAggregationV3Task.name       => provide(statsAggV3TaskHandler)
      case RtbStatsExtendedTask.name            => provide(statsExtendedTaskHandler)
      case RtbStatsExtendedP3Task.name          => provide(statsExtendedP3TaskHandler)
      case RtbStatsExtendedP3CsvTask.name       => provide(statsExtendedP3CsvTaskHandler)
      case RtbStatsP3CsvTask.name               => provide(statsP3CsvTaskHandler)
      case RtbStatsMappingP3CsvTask.name        => provide(statsMappingP3CsvTaskHandler)
      case _                                    => reject(UnsupportedTask(name))
    }
  }
}
