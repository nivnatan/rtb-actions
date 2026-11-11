package com.rtb.admin.routes.users.models

import akka.http.scaladsl.server.Rejection

/**
 * Created by Niv on 10/11/2026
 */
sealed trait UserActionResult
case class UserActionSuccess(json: String) extends UserActionResult
abstract class UserActionError(_id: String, val errorMsg: String) extends Throwable with Rejection with UserActionResult { def id: String = s"USER_ACTION_${_id}_ERROR" }