package com.rtb.actions.routes.error

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes.BadRequest
import akka.http.scaladsl.server.Directives.{complete, extractRequest}
import akka.http.scaladsl.server.{RejectionError, Route}
import com.common.routes.MyRouteErrorHandler
import com.common.utils.logging.LoggingSupport
import com.rtb.actions.config.ConfigSupport
import com.rtb.actions.routes.actions.models.ActionError
import com.rtb.actions.utils.counters.Counters.RtbActionsRouteErrorsCount

/**
  * Created by Niv on 11/12/2021
  */
trait ActionErrorHandler extends MyRouteErrorHandler with LoggingSupport {

  this: ConfigSupport =>

  override protected def onError(err: Throwable): Route = {
    onError(err)
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
