package com.rtb.admin.routes.users.handlers.bundles.rejections

import com.common.rtb.dao.RtbDao
import com.rtb.admin.constants.Operations.{Add, Delete, Replace}
import com.rtb.admin.routes.users.constants.UserActionBundlesRejections.{BundleListIsEmptyRequestRejectionType, UserActionBundlesRejection}
import com.rtb.admin.routes.users.handlers.bundles.models.BundlesRequest

/**
 * Created by Niv on 18/11/2026
 */
class BundlesRejections(dao: RtbDao) {

  trait UserActionBundlesRequestRejection {
    def rejectionType: UserActionBundlesRejection
    def pass(bundleRequest: BundlesRequest): Boolean
  }

  private object BundleListIsEmptyRequestRejection extends UserActionBundlesRequestRejection {
    def rejectionType: UserActionBundlesRejection = BundleListIsEmptyRequestRejectionType
    def pass(bundleRequest: BundlesRequest): Boolean =
      bundleRequest.userActionRequest.operation match {
        case Some(Add) | Some(Replace) | Some(Delete) => bundleRequest.bundles.nonEmpty
        case _ => true
      }
  }

  def pass(bundleRequest: BundlesRequest): Boolean =
    rejections
      .forall { rej =>
        val isPass = rej.pass(bundleRequest)
        isPass
      }

  private final val rejections: Vector[UserActionBundlesRequestRejection] = Vector(
    BundleListIsEmptyRequestRejection
  )
}