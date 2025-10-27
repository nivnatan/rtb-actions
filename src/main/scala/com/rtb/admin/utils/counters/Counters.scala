package com.rtb.admin.utils.counters

import com.common.utils.counters.Counter

/**
  * Created by niv on 12/13/2021
  */
object Counters {

  case object RtbActionsRouteRequestsCount                      extends Counter("RtbActionsRouteRequestsCount")
  case object RtbActionsRouteErrorsCount                        extends Counter("RtbActionsRouteErrorsCount")
  case object RtbActionsBucketReplaceRequestsSuccessCount       extends Counter("RtbActionsBucketReplaceRequestsSuccessCount")
  case object RtbActionsBucketReplaceRequestsFailureCount       extends Counter("RtbActionsBucketReplaceRequestsFailureCount")
  case object RtbActionsBucketAddRequestsSuccessCount           extends Counter("RtbActionsBucketAddRequestsSuccessCount")
  case object RtbActionsBucketAddRequestsFailureCount           extends Counter("RtbActionsBucketAddRequestsFailureCount")

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
