package com.rtb.tasks.routes.public

import akka.http.scaladsl.server.Directives.path
import com.rtb.tasks.config.{Config, ConfigSupport}
import com.rtb.tasks.routes.error.TaskErrorHandler
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import com.common.routes.CorsHandler
import scala.language.postfixOps

/**
 * Created by Niv on 02/12/2024
 */
case class PublicRoute(config: Config) extends ConfigSupport with TaskErrorHandler {

  lazy val route: Route =
    (path("public" / Segment) & get) { fileName =>
      extractRequest { httpRequest =>
        respondWithHeaders(CorsHandler.getAccessControlHeaders(httpRequest.headers)) {
          getFromResource(s"public/$fileName")
        }
      }
    }
}