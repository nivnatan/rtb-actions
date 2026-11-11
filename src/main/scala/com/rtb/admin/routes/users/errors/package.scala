package com.rtb.admin.routes.users

import akka.http.scaladsl.server.RejectionError
import com.rtb.admin.routes.users.constants.UserActionErrors.UnknownError
import com.rtb.admin.routes.users.models.UserActionError
import scala.util.control.NonFatal

/**
 * Created by Niv on 10/11/2026
 */
package object errors {

  implicit class ThrowableOps(private val t: Throwable) extends AnyVal {
    def toUserActionError: UserActionError = t match {
      case e: UserActionError                         => e
      case RejectionError(inner: UserActionError)     => inner
      case NonFatal(e)                                => UnknownError(e.getMessage)
      case _                                          => UnknownError(t.getClass.getSimpleName)
    }
  }
}
