package com.rtb.admin.routes.actions

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{as, authenticateBasicAsync, entity, pathPrefix}
import akka.http.scaladsl.server.{Directive1, Route}
import akka.http.scaladsl.server.directives.BasicDirectives
import com.common.routes.{extractParams, isDebug}
import com.common.utils.http.HttpUtil
import com.common.utils.time.MyLocalDateTime
import com.rtb.admin.config.{Config, ConfigSupport}
import com.rtb.admin.routes.actions.models.{ActionError, ActionRequest, ActionResult, ActionSuccess}
import akka.http.scaladsl.server.Directives._
import com.rtb.admin.routes.actions.constants.ActionErrors.UnknownActionType
import com.rtb.admin.routes.actions.constants.Actions
import com.rtb.admin.routes.actions.constants.Actions.Action
import com.rtb.admin.routes.actions.error.ActionErrorHandler
import com.rtb.admin.routes.actions.handlers.ActionHandlers
import com.rtb.admin.utils.counters.Counters.RtbActionsRouteRequestsCount

import scala.util.{Failure, Success, Try}

/**
 * Created by Niv on 17/10/2025
 */
case class ActionsRoute(config: Config) extends ConfigSupport with ActionErrorHandler {

  private val actionHandlers = new ActionHandlers(config)

  lazy val route: Route = {
    withErrorSupport {
      pathPrefix("admin" / "actions" / Segment) { action =>
        authenticateBasicAsync(realm = "secure site", HttpUtil.authenticate("rapidadmin1234!")) { _ =>
          extractRequest(action) { adminRequest =>
            countersHandler ! RtbActionsRouteRequestsCount
            debug(s"$adminRequest")
            respond(adminRequest, Try{actionHandlers.getHandler(adminRequest.action).handle(adminRequest)})
          }
        }
      }
    }
  }

  private def extractRequest(action: String): Directive1[ActionRequest] = {
    for {
      httpRequest <- BasicDirectives.extractRequest
      payload     <- entity(as[String])
      action      <- Actions.getAction(action).map(provide).getOrElse(reject(UnknownActionType)): Directive1[Action]
      debug       <- isDebug
      params      <- extractParams
    } yield ActionRequest(params, MyLocalDateTime.now, payload, action, debug)
  }

  private def respond(request: ActionRequest, response: Try[ActionResult]): Route = {
    debug(s"$response")

    response match {
      case Success(ActionSuccess(payloadJson)) =>
        val body = s"""{"status":1,"data":$payloadJson}"""
        info(body)
        complete(
          HttpEntity(
            ContentTypes.`application/json`,
            body
          )
        )

      case Success(e: ActionError)  =>
        onError(e)

      case Failure(e)  =>
        onError(e)
    }
  }
}