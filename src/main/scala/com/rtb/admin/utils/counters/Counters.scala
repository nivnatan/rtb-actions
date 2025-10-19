package com.rtb.admin.utils.counters

import com.common.utils.counters.Counter

/**
  * Created by niv on 12/13/2021
  */
object Counters {

  case object RtbActionsRouteRequestsCount                      extends Counter("RtbActionsRouteRequestsCount")
  case object RtbActionsRouteErrorsCount                        extends Counter("RtbActionsRouteErrorsCount")
  case object RtbActionsBucketReplaceRequestsSuccessCount       extends Counter("RtbActionsBucketReplaceRequestsCount")
  case object RtbActionsBucketReplaceRequestsFailureCount       extends Counter("RtbActionsBucketReplaceRequestsCount")
  case object RtbActionsBucketAddRequestsSuccessCount           extends Counter("RtbActionsBucketReplaceRequestsCount")
  case object RtbActionsBucketAddRequestsFailureCount           extends Counter("RtbActionsBucketReplaceRequestsCount")

  def toSet: Set[Counter] =
    Set(
      RtbActionsRouteRequestsCount,
      RtbActionsRouteErrorsCount,
      RtbActionsBucketReplaceRequestsSuccessCount,
      RtbActionsBucketReplaceRequestsFailureCount,
      RtbActionsBucketAddRequestsSuccessCount,
      RtbActionsBucketAddRequestsFailureCount
    )
}
