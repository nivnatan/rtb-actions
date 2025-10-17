package com.rtb.tasks.routes

import akka.http.scaladsl.server.Route
import com.rtb.tasks.config.Config
import com.rtb.tasks.routes.images.ImageRoute
import akka.http.scaladsl.server.Directives._
import com.rtb.tasks.routes.beacon.BeaconRoute
import com.rtb.tasks.routes.encoding.EncodingRoute
import com.rtb.tasks.routes.public.PublicRoute
import com.rtb.tasks.routes.redirect.RedirectRoute
import com.rtb.tasks.routes.tasks.TasksRoute

/**
  * Created by niv on 12/13/2021
  */
trait TasksRoutes {
  lazy val routes                                : Route = tasksRoute ~ imageRoute ~ beaconRoute ~ encodingRoute ~ redirectRoute ~ publicRoute
  protected def tasksRoute                       : Route = TasksRoute(config).route
  protected def imageRoute                       : Route = ImageRoute(config).route
  protected def beaconRoute                      : Route = BeaconRoute(config).route
  protected def encodingRoute                    : Route = EncodingRoute(config).route
  protected def redirectRoute                    : Route = RedirectRoute(config).route
  protected def publicRoute                      : Route = PublicRoute(config).route

  protected def config: Config
}
