package com.rtb.tasks.routes.encoding

import akka.http.scaladsl.server.Directives.{complete, path, _}
import akka.http.scaladsl.server.Route
import com.common.utils.encoding.EncodingUtil
import com.rtb.tasks.config.{Config, ConfigSupport}
import com.rtb.tasks.routes.error.TaskErrorHandler

/**
  * Created by Niv on 20/07/2023
  */
case class EncodingRoute(config: Config) extends ConfigSupport with TaskErrorHandler {

  lazy val route: Route =
    withErrorSupport {
      (path("encoding") & get) {
        parameter("str") { str =>
          complete(EncodingUtil.myEncryptString(str))
        }
      }
    }
}