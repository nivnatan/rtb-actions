package com.rtb.admin.routes.users.handlers.bundles

import com.common.utils.json.JsonUtil
import com.rtb.admin.config.{Config, ConfigSupport}
import com.rtb.admin.constants.Operations._
import com.rtb.admin.routes.users.constants.UserActionBundlesErrors.{BundleOperationFailedError, InitBundleRequestError, InvalidOperationError}
import com.rtb.admin.routes.users.handlers.UserActionHandler
import com.rtb.admin.routes.users.handlers.bundles.BundlesHandler.BundlesPayload
import com.rtb.admin.routes.users.handlers.bundles.models.BundlesRequest
import com.rtb.admin.routes.users.handlers.bundles.rejections.BundlesRejections
import com.rtb.admin.routes.users.models.{UserActionRequest, UserActionResult, UserActionSuccess}
import com.common.utils.types.TypesUtil._
import scala.util.{Failure, Success}

/**
 * Created by Niv on 10/11/2026
 */
object BundlesHandler {
  case class BundlesPayload(bundles: List[String])
}

class BundlesHandler(val config: Config) extends UserActionHandler with ConfigSupport {

  private val rejections = new BundlesRejections(dao)

  override def handle(userRequest: UserActionRequest): UserActionResult = {
    initBundlesRequest(userRequest).filter(rejections.pass).map { bundleRequest =>
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
    bucketsService.bucketsGet.get(bundleRequest.bucketId) match {
      case Success(values) => JsonUtil.toJson(Map("bundles" -> values)).fold[UserActionResult](e => BundleOperationFailedError(e), json => UserActionSuccess(json))
      case Failure(e)      => BundleOperationFailedError(e)
    }
  }

  private def add(bundleRequest: BundlesRequest): UserActionResult ={
    bucketsService.bucketsAdd.add(bundleRequest.bucketId, bundleRequest.bundles)
      .fold(e => BundleOperationFailedError(e), r => UserActionSuccess(s"""{"addedCount":${r.addedCount},"totalCount":${r.totalCount}}"""))
  }

  private def replace(bundleRequest: BundlesRequest): UserActionResult ={
    bucketsService.bucketsReplace.replace(bundleRequest.bucketId, bundleRequest.bundles)
      .fold(e => BundleOperationFailedError(e), r => UserActionSuccess(s"""{"totalCount":$r}"""))
  }

  private def delete(bundleRequest: BundlesRequest): UserActionResult ={
    bucketsService.bucketsDelete.delete(bundleRequest.bucketId, bundleRequest.bundles)
      .fold(e => BundleOperationFailedError(e), r => UserActionSuccess(s"""{"deletedCount":${r.deletedCount},"totalBefore":${r.totalBefore},"totalAfter":${r.totalAfter}}"""))
  }

  private def initBundlesRequest(userRequest: UserActionRequest): Option[BundlesRequest] = {
    val bucketId: Option[Long] = (userRequest.dspData, userRequest.sspData, userRequest.params.get("mode").map(_.toLowerCase)) match {
      case (Some(dspData), None, Some("whitelist")) => dspData.whitelistAppBundlesBucketIds.headOption.flatMap(_.toLongSafe)
      case (Some(dspData), None, Some("blacklist")) => dspData.blacklistAppBundlesBucketIds.headOption.flatMap(_.toLongSafe)
      case (None, Some(sspData), Some("blacklist")) => sspData.blacklistAppBundlesBucketIds.headOption.flatMap(_.toLongSafe)
      case _ => None
    }

    val bundles: Set[String] =
      JsonUtil.fromJson[BundlesPayload](userRequest.payload)
        .toOption
        .map(_.bundles.map(_.trim.toLowerCase(java.util.Locale.ROOT)).filter(_.nonEmpty).toSet)
        .getOrElse(Set.empty)

    bucketId.map(BundlesRequest(_, userRequest, bundles))
  }
}
