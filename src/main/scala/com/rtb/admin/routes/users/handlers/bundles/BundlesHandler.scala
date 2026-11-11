package com.rtb.admin.routes.users.handlers.bundles

import com.rtb.admin.config.{Config, ConfigSupport}
import com.rtb.admin.constants.Operations._
import com.rtb.admin.routes.users.constants.UserActionBundlesErrors.{InitBundleRequestError, InvalidOperationError}
import com.rtb.admin.routes.users.handlers.UserActionHandler
import com.rtb.admin.routes.users.models.{UserActionRequest, UserActionResult, UserActionSuccess}

/**
 * Created by Niv on 10/11/2026
 */
class BundlesHandler(val config: Config) extends UserActionHandler with ConfigSupport {

  private  case class BundlesRequest(bucketId: String, userActionRequest: UserActionRequest)

  override def handle(userRequest: UserActionRequest): UserActionResult = {
    initBundlesRequest(userRequest).map { bundleRequest =>
      userRequest.operation match {
        case Some(Get)        => get(bundleRequest)
        case Some(Add)        => add(bundleRequest)
        case Some(Replace)    => replace(bundleRequest)
        case Some(Delete)     => delete(bundleRequest)
        case _                => InvalidOperationError
      }
    }.getOrElse(InitBundleRequestError)
  }

  private def get(bundleRequest: BundlesRequest): UserActionResult ={
    UserActionSuccess(s"""{"status":1,"data":"GET"""")
  }

  private def add(bundleRequest: BundlesRequest): UserActionResult ={
    UserActionSuccess(s"""{"status":1,"data":"ADD"""")
  }

  private def replace(bundleRequest: BundlesRequest): UserActionResult ={
    UserActionSuccess(s"""{"status":1,"data":"REPLACE"""")
  }

  private def delete(bundleRequest: BundlesRequest): UserActionResult ={
    UserActionSuccess(s"""{"status":1,"data":"DELETE"""")
  }

  private def initBundlesRequest(userRequest: UserActionRequest): Option[BundlesRequest] = {
    val bucketId: Option[String] = (userRequest.dspData, userRequest.sspData, userRequest.params.get("mode").map(_.toLowerCase)) match {
      case (Some(dspData), None, Some("whitelist")) => dspData.whitelistAppBundlesBucketIds.headOption
      case (Some(dspData), None, Some("blacklist")) => dspData.blacklistAppBundlesBucketIds.headOption
      case (None, Some(sspData), Some("blacklist")) => sspData.blacklistAppBundlesBucketIds.headOption
      case _ => None
    }

    bucketId.map(BundlesRequest(_, userRequest))
  }
}
