package com.rtb.tasks.routes.tasks

import akka.http.scaladsl.server.Directives.path
import com.rtb.tasks.config.{Config, ConfigSupport}
import com.rtb.tasks.routes.error.TaskErrorHandler
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.{Directive1, Route}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.BasicDirectives
import scala.language.postfixOps
import com.common.utils.time.MyLocalDateTime
import com.rtb.tasks.routes.tasks.handlers.TaskHandlers
import com.rtb.tasks.routes.tasks.models.TaskRequest
import com.rtb.tasks.utils.counters.Counters.RtbTaskRouteCount
import com.common.routes._

/**
  * Created by Niv on 11/12/2021
  */
case class TasksRoute(config: Config) extends ConfigSupport with TaskErrorHandler {

  private val handlers = new TaskHandlers(config)

  lazy val route: Route =
    withErrorSupport {
      (path("rtb" / "task" / Segment) & (get | post)) { taskName =>
        countersHandler ! RtbTaskRouteCount
        extractRequest { taskRequest =>
          handlers.getHandler(taskName) { handler =>
            handler.handle(taskRequest)
            complete(taskRequest.debug.map(_.toToResponseMarshallable).getOrElse("Done."))
          }
        }
      }
    }

  private def extractRequest: Directive1[TaskRequest] = {
    for {
      httpRequest <- BasicDirectives.extractRequest
      payload     <- entity(as[String])
      debug       <- isDebug
    } yield TaskRequest(httpRequest.uri.query().toMap, MyLocalDateTime.now, payload, debug)
  }
}
