package com.rtb.actions.routes

import akka.http.scaladsl.server.Route
import com.rtb.actions.config.Config
import com.rtb.actions.routes.actions.ActionsRoute

/**
  * Created by niv on 12/13/2021
  */
trait ActionsRoutes {
  lazy val routes                                : Route = actionsRoute
  protected def actionsRoute                     : Route = ActionsRoute(config).route

  protected def config: Config
}
