package com.rtb.tasks.routes.beacon

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.path
import com.rtb.tasks.config.{Config, ConfigSupport}
import com.rtb.tasks.routes.error.TaskErrorHandler
import akka.http.scaladsl.server.{Directive, Directive0, Route}
import akka.http.scaladsl.server.Directives._
import com.common.routes.CorsHandler
import com.common.utils.encoding.EncodingUtil
import com.common.utils.http.KeepAliveHeader
import scala.language.postfixOps

/**
  * Created by Niv on 21/06/2022
  */
case class BeaconRoute(config: Config) extends ConfigSupport with TaskErrorHandler {

  lazy val route: Route =
    (path("beacon" / Segment) & get) { beaconName =>
      parameter("red") { redirectUrlEnc =>
        withHeaders() {
          redirect(EncodingUtil.myDecryptString(redirectUrlEnc), StatusCodes.PermanentRedirect)
        }
      }
    } ~ (path("beacons" / Segment) & get) { beaconName =>
      complete(StatusCodes.NoContent)
    }

  def withHeaders(): Directive0 = {
    Directive { inner ⇒
      extractRequest { httpRequest =>
        respondWithHeaders(CorsHandler.getAccessControlHeaders(httpRequest.headers) ++ List(KeepAliveHeader)) {
          inner()
        }
      }
    }
  }
}