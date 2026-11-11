package com.rtb.admin.routes.users.handlers

import com.rtb.admin.config.Config
import com.rtb.admin.routes.users.constants.UserActions._
import com.rtb.admin.routes.users.handlers.bundles.BundlesHandler
import com.rtb.admin.routes.users.models.{UserActionRequest, UserActionResult}

/**
 * Created by Niv on 10/11/2026
 */
trait UserActionHandler {
  def handle(userActionRequest: UserActionRequest): UserActionResult
}

class UserActionHandlers(config: Config) {

  private val bundlesHandler    = new BundlesHandler(config)

  def getHandler(action: UserAction): UserActionHandler =
    action match {
      case Bundles            => bundlesHandler
    }
}