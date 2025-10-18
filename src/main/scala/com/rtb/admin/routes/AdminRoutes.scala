package com.rtb.admin.routes

import akka.http.scaladsl.server.Route
import com.rtb.admin.config.Config
import com.rtb.admin.routes.actions.ActionsRoute

/**
  * Created by niv on 12/13/2021
  */
trait AdminRoutes {
  lazy val routes                                : Route = actionsRoute
  protected def actionsRoute                     : Route = ActionsRoute(config).route

  protected def config: Config
}
