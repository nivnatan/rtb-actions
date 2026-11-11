package com.rtb.admin.routes.users

import akka.http.scaladsl.model.StatusCodes
import com.rtb.admin.routes.users.models.UserActionError

/**
 * Created by Niv on 10/11/2026
 */
package object constants {

  object UserActions {
    sealed abstract class UserAction(val name: String)
    case object Bundles extends UserAction("bundles")

    def getAction(name: String): Option[UserAction] = {
      name.toLowerCase match {
        case Bundles.name       => Some(Bundles)
        case _                        => None
      }
    }
  }

  object UserActionRouteErrors {
    sealed abstract class UserActionRouteError(_id: String, errorMsg: String, errorCode: Int) extends UserActionError(s"ROUTE_${_id}", errorMsg, StatusCodes.BadRequest, errorCode)
    case object InvalidCredentialsError extends UserActionRouteError("INVALID_CREDENTIALS", "invalid credentials", 1)
    case object InvalidActionError extends UserActionRouteError("INVALID_ACTION", "invalid action type", 2)
  }

  object UserActionRejections {
    sealed abstract class UserActionRejection(_id: String, errorMsg: String, errorCode: Int) extends UserActionError(s"${_id}_REJECTION", errorMsg, StatusCodes.BadRequest, errorCode)
    case object AuthenticationRejectionType extends UserActionRejection("AUTHENTICATION", "authentication failed", 3)
  }

  object UserActionErrors {
    case class UnknownError(error: String) extends UserActionError("UNKNOWN", error, StatusCodes.InternalServerError, 4)
  }

  object UserActionBundlesErrors {
    case object InvalidOperationError extends UserActionError("INVALID_OPERATION", "operation is invalid", StatusCodes.BadRequest, 5)
    case object InitBundleRequestError extends UserActionError("INIT_BUNDLES_REQUEST", "init bundles request error", StatusCodes.BadRequest, 6)
  }
}
