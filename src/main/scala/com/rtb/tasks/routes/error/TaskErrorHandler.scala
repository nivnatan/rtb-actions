package com.rtb.tasks.routes.error

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.server.Directives.{complete, extractRequest}
import akka.http.scaladsl.server.Route
import com.common.routes.MyRouteErrorHandler
import com.common.utils.logging.LoggingSupport
import com.rtb.tasks.config.ConfigSupport
import com.rtb.tasks.utils.counters.Counters.RtbTaskRouteErrorCount
import scala.util.Try

/**
  * Created by Niv on 11/12/2021
  */
trait TaskErrorHandler extends MyRouteErrorHandler with LoggingSupport {

  this: ConfigSupport =>

  override protected def onError(err: Throwable): Route = {
    countersHandler ! RtbTaskRouteErrorCount
    extractRequest { req =>
      val errorMsg = s"task route error. err=$err, uri=${Try{req.uri.toString}}"
      error(errorMsg)
      complete(StatusCodes.custom(InternalServerError.intValue, InternalServerError.reason, errorMsg))
    }
  }
}
