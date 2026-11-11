package com.rtb.admin.routes.users.rejections

import akka.http.scaladsl.server.{Directive0, Directives}
import akka.http.scaladsl.server.Directives.reject
import com.common.rtb.dao.RtbDao
import com.rtb.admin.routes.users.constants.UserActionRejections.{AuthenticationRejectionType, UserActionRejection}
import com.rtb.admin.routes.users.models.UserActionRequest

/**
 * Created by Niv on 10/11/2026
 */
class UserRejections(dao: RtbDao) {

  trait UserRouteRequestRejection {
    def rejectionType: UserActionRejection
    def pass(userActionRequest: UserActionRequest): Boolean
  }

  private object AuthenticationRejection extends UserRouteRequestRejection {
    def rejectionType: UserActionRejection = AuthenticationRejectionType
    def pass(userRequest: UserActionRequest): Boolean =
      userRequest.password.nonEmpty && userRequest.password == userRequest.adminUserData.password
  }

  def pass(userRequest: UserActionRequest): Directive0 =
    rejections.find(!_.pass(userRequest)) match {
      case Some(r) => reject(r.rejectionType)
      case _       => Directives.pass
    }

  private final val rejections: Vector[UserRouteRequestRejection] = Vector(
    AuthenticationRejection
  )
}
