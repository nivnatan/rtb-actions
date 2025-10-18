package com.rtb.admin.routes.actions.error

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes.BadRequest
import akka.http.scaladsl.server.Directives.{complete, extractRequest}
import akka.http.scaladsl.server.Route
import com.common.routes.MyRouteErrorHandler
import com.common.utils.logging.LoggingSupport
import com.rtb.admin.config.ConfigSupport
import com.rtb.admin.routes.actions.models.ActionError
import com.rtb.admin.utils.counters.Counters.RtbActionsRouteErrorsCount
import ActionErrorOps._

/**
  * Created by Niv on 11/12/2021
  */
trait ActionErrorHandler extends MyRouteErrorHandler with LoggingSupport {

  this: ConfigSupport =>

  override protected def onError(err: Throwable): Route = {
    onError(err.toActionError)
  }

  protected def onError(err: ActionError): Route = {
    countersHandler ! RtbActionsRouteErrorsCount
    extractRequest { _ =>
      val body = s"""{"status":0,"errorId":"${err.id}","errorMsg":"${err.errorMsg}"}"""
      error(body)
      complete(StatusCodes.custom(BadRequest.intValue, BadRequest.reason, body))
    }
  }
}
