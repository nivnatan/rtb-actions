package com.rtb.tasks.routes.redirect

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.{complete, path, _}
import akka.http.scaladsl.server.Route
import com.rtb.tasks.config.{Config, ConfigSupport}
import com.rtb.tasks.routes.error.TaskErrorHandler
import com.common.routes._
import com.common.utils.encoding.EncodingUtil

/**
 * Created by Niv on 01/12/2024
 */
case class RedirectRoute(config: Config) extends ConfigSupport with TaskErrorHandler {

  lazy val route: Route =
    (path("red") & get) {
      getUrl {
        case Some(_url) => complete(HttpResponse(status = StatusCodes.Found, akka.http.scaladsl.model.headers.Location(_url) :: Nil))
        case _          => complete(StatusCodes.NoContent)
      }
    }

  private def getUrl = {
    extractParamsEncrypted("end") flatMap { map =>
       provide(map.get("u") orElse map.get("uen").flatMap(EncodingUtil.myDecryptStringSafe))
    }
  }
}