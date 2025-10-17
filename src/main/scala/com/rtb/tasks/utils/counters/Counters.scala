package com.rtb.tasks.utils.counters

import com.common.utils.counters.Counter

/**
  * Created by niv on 12/13/2021
  */
object Counters {

  case object RtbTaskRouteCount                               extends Counter("RtbTaskRouteCount")
  case object RtbTaskRouteErrorCount                          extends Counter("RtbTaskRouteErrorCount")
  case object StatsAggregationV2TaskQuerySuccessCount         extends Counter("StatsAggregationV2TaskQuerySuccessCount")
  case object StatsAggregationV2TaskQueryFailureCount         extends Counter("StatsAggregationV2TaskQueryFailureCount")
  case object StatsAggregationV3TaskQuerySuccessCount         extends Counter("StatsAggregationV3TaskQuerySuccessCount")
  case object StatsAggregationV3TaskQueryFailureCount         extends Counter("StatsAggregationV3TaskQueryFailureCount")

  def toSet: Set[Counter] =
    Set(
      RtbTaskRouteCount,
      RtbTaskRouteErrorCount,
      StatsAggregationV2TaskQuerySuccessCount,
      StatsAggregationV2TaskQueryFailureCount,
      StatsAggregationV3TaskQuerySuccessCount,
      StatsAggregationV3TaskQueryFailureCount
    )
}
