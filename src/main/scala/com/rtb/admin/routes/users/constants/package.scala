package com.rtb.admin.routes.users

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
    sealed abstract class UserActionRouteError(_id: String, errorMsg: String) extends UserActionError(s"ROUTE_${_id}", errorMsg)
    case object InvalidCredentialsError extends UserActionRouteError("INVALID_CREDENTIALS", "invalid credentials")
    case object InvalidActionError extends UserActionRouteError("INVALID_ACTION", "invalid action type")
  }

  object UserActionRejections {
    sealed abstract class UserActionRejection(_id: String, errorMsg: String) extends UserActionError(s"${_id}_REJECTION", errorMsg)
    case object AuthenticationRejectionType extends UserActionRejection("AUTHENTICATION", "authentication failed")
  }

  object UserActionErrors {
    case class UnknownError(error: String) extends UserActionError("UNKNOWN", error)
  }
}
