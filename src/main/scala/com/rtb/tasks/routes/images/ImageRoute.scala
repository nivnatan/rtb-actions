package com.rtb.tasks.routes.images

import akka.http.scaladsl.server.Directives.path
import com.rtb.tasks.config.{Config, ConfigSupport}
import com.rtb.tasks.routes.error.TaskErrorHandler
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import com.common.utils.encoding.EncodingUtil
import scala.language.postfixOps

/**
  * Created by Niv on 21/06/2022
  */
case class ImageRoute(config: Config) extends ConfigSupport with TaskErrorHandler {

  lazy val route: Route =
    (path("img" / Segment) & get) { imageName =>
      getFromResource(s"images/$imageName")
    } ~
      (path("imgenc" / Segment) & get) { imageName =>
        getFromResource(s"images/${EncodingUtil.myDecryptString(imageName)}")
      } ~
      (path("vid" / Segment) & get) { imageName =>
        getFromResource(s"videos/$imageName")
      }
}