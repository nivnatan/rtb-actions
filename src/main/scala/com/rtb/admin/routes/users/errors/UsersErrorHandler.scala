package com.rtb.admin.routes.users.errors

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.{complete, extractRequest}
import akka.http.scaladsl.server.Route
import com.common.routes.MyRouteErrorHandler
import com.common.utils.logging.LoggingSupport
import com.rtb.admin.config.ConfigSupport
import com.rtb.admin.routes.users.constants.UserActionRejections.UserActionRejection
import com.rtb.admin.routes.users.constants.UserActionRouteErrors.UserActionRouteError
import com.rtb.admin.routes.users.models.UserActionError
import com.rtb.admin.utils.counters.Counters.RtbUserActionsRouteErrorsCount

/**
 * Created by Niv on 10/11/2026
 */
trait UsersErrorHandler extends MyRouteErrorHandler with LoggingSupport {

  this: ConfigSupport =>

  override protected def onError(err: Throwable): Route = {
    onError(err.toUserActionError)
  }

  protected def onError(err: UserActionError): Route = {
    countersHandler ! RtbUserActionsRouteErrorsCount
    extractRequest { _ =>
      error(err.errorMsg)
      complete(
        HttpResponse(
          status  = err.statusCode,
          headers = List(RawHeader("X-Error-Code", err.errorCode.toString))
        )
      )
    }
  }
}