package com.rtb.admin.routes.users

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.server.Directives.pathPrefix
import akka.http.scaladsl.server.{Directive1, Route}
import com.rtb.admin.config.{Config, ConfigSupport}
import com.rtb.admin.routes.users.errors.UsersErrorHandler
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.BasicDirectives
import com.common.routes.{extractParams, isDebug}
import com.common.rtb.models.data.RtbAdminUserData
import com.common.utils.time.MyLocalDateTime
import com.rtb.admin.constants.Operations
import com.rtb.admin.routes.users.constants.UserActionRouteErrors.InvalidCredentialsError
import com.rtb.admin.routes.users.constants.{UserActionRouteErrors, UserActions}
import com.rtb.admin.routes.users.constants.UserActions.UserAction
import com.rtb.admin.routes.users.handlers.UserActionHandlers
import com.rtb.admin.routes.users.models.{UserActionError, UserActionRequest, UserActionResult, UserActionSuccess}
import com.rtb.admin.routes.users.rejections.UserRejections
import com.rtb.admin.utils.counters.Counters.RtbUserActionsRouteRequestsCount
import scala.util.{Failure, Success, Try}

/**
 * Created by Niv on 10/11/2026
 */
case class UsersRoute(config: Config) extends ConfigSupport with UsersErrorHandler {

  private val handlers    = new UserActionHandlers(config)
  private val rejections  = new UserRejections(dao)

  lazy val route: Route = {
    withErrorSupport {
      path("users" / "actions" / Segment) { action =>
        extractRequest(action) { userRequest =>
          countersHandler ! RtbUserActionsRouteRequestsCount
          debug(s"$userRequest")
          rejections.pass(userRequest) {
            respond(userRequest, Try {
              handlers.getHandler(userRequest.action).handle(userRequest)
            })
          }
        }
      }
    }
  }

  private def extractRequest(action: String): Directive1[UserActionRequest] = {
    for {
      params          <- extractParams
      time            <- provide(MyLocalDateTime.now)
      credentials     <- extractCredentials.flatMap { case Some(c: BasicHttpCredentials) => provide(c) case _ => reject(InvalidCredentialsError) }: Directive1[BasicHttpCredentials]
      adminUserData   <- dao.getRtbAdminUserDataByUser(credentials.username).map(provide).getOrElse(reject(UserActionRouteErrors.InvalidCredentialsError): Directive1[RtbAdminUserData])
      action          <- UserActions.getAction(action).map(provide).getOrElse(reject(UserActionRouteErrors.InvalidActionError)): Directive1[UserAction]
      operation       <- extractMethod.map(Operations.get)
      password        <- provide(credentials.password)
      debug           <- isDebug
      payload         <- entity(as[String])
    } yield UserActionRequest(params, time, payload, adminUserData, action, operation, password, debug)
  }

  private def respond(request: UserActionRequest, response: Try[UserActionResult]): Route = {
    debug(s"$response")

    response match {
      case Success(UserActionSuccess(payloadJson)) =>
        val body = s"""{"status":1,"data":$payloadJson}"""
        info(body)
        complete(
          HttpEntity(
            ContentTypes.`application/json`,
            body
          )
        )

      case Success(e: UserActionError)  =>
        onError(e)

      case Failure(e)  =>
        onError(e)
    }
  }
}
