package com.rtb.actions.routes.actions.models
import akka.http.scaladsl.server.Rejection

/**
 * Created by Niv on 11/12/2021
 */
sealed trait ActionResult
case class ActionSuccess(json: String) extends ActionResult
trait ActionError extends Throwable with Rejection with ActionResult { def id: String; def errorMsg: String; override def toString: String = s"$id: $errorMsg" }