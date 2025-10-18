package com.rtb.admin.routes.actions.error

import akka.http.scaladsl.server.RejectionError
import com.rtb.admin.routes.actions.constants.ActionErrors.UnknownActionError
import com.rtb.admin.routes.actions.models.ActionError
import scala.util.control.NonFatal

/**
 * Created by Niv on 11/12/2021
 */
object ActionErrorOps {
  implicit class ThrowableOps(private val t: Throwable) extends AnyVal {
    def toActionError: ActionError = t match {
      case e: ActionError                     => e
      case RejectionError(inner: ActionError) => inner
      case NonFatal(e)                        => UnknownActionError(e.getMessage)
      case _                                  => UnknownActionError(t.getClass.getSimpleName)
    }
  }
}
