package com.rtb.actions.utils.counters

import com.common.utils.counters.Counter

/**
  * Created by niv on 12/13/2021
  */
object Counters {

  case object RtbActionsRouteRequestsCount                      extends Counter("RtbActionsRouteRequestsCount")
  case object RtbActionsRouteErrorsCount                        extends Counter("RtbActionsRouteErrorsCount")

  def toSet: Set[Counter] =
    Set(
      RtbActionsRouteRequestsCount,
      RtbActionsRouteErrorsCount
    )
}
