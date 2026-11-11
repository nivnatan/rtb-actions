package com.rtb.admin.routes

import akka.http.scaladsl.server.Route
import com.rtb.admin.config.Config
import com.rtb.admin.routes.actions.ActionsRoute
import com.rtb.admin.routes.users.UsersRoute
import akka.http.scaladsl.server.Directives._

/**
  * Created by niv on 12/13/2021
  */
trait AdminRoutes {
  lazy val routes                                 : Route = actionsRoute ~ usersRoute
  protected def actionsRoute                      : Route = ActionsRoute(config).route
  protected def usersRoute                        : Route = UsersRoute(config).route

  protected def config: Config
}
