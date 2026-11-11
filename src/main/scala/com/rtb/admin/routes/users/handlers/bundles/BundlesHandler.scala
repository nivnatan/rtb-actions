package com.rtb.admin.routes.users.handlers.bundles

import com.rtb.admin.config.{Config, ConfigSupport}
import com.rtb.admin.routes.users.handlers.UserActionHandler
import com.rtb.admin.routes.users.models.{UserActionRequest, UserActionResult, UserActionSuccess}

import scala.concurrent.Future

/**
 * Created by Niv on 10/11/2026
 */
class BundlesHandler(val config: Config) extends UserActionHandler with ConfigSupport {

  override def handle(userActionRequest: UserActionRequest): UserActionResult = ???

  def get(userActionRequest: UserActionRequest): UserActionResult ={
    ???
  }

  def add(userActionRequest: UserActionRequest): UserActionResult ={
    ???
  }

  def replace(userActionRequest: UserActionRequest): UserActionResult ={
    ???
  }

  def delete(userActionRequest: UserActionRequest): UserActionResult ={
    ???
  }
}
