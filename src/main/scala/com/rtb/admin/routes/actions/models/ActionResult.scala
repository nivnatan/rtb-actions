package com.rtb.admin.routes.actions.models
import akka.http.scaladsl.server.Rejection

/**
 * Created by Niv on 11/12/2021
 */
sealed trait ActionResult
case class ActionSuccess(json: String) extends ActionResult
trait ActionError extends Throwable with Rejection with ActionResult { def id: String; def errorMsg: String; override def toString: String = s"$id: $errorMsg" }

object ActionErrors {

  sealed abstract class ActionErrorType(_id: String, val errorMsg: String) extends ActionError { override def id: String = s"ACTION_${_id}" }
  case class ActionUnknownError(error: String) extends ActionErrorType("UNKNOWN_ERROR", error)
  case object UnknownActionType extends ActionErrorType("UNKNOWN_ACTION_TYPE", "unknown action type")

  object BucketsErrorTypes {
    sealed abstract class BucketErrorType(_id: String, errorMsg: String) extends ActionErrorType(s"BUCKET_${_id}", errorMsg)
    case object InvalidBucketParameters extends BucketErrorType("INVALID_BUCKET_PARAMETERS", "dsa")
    case class SqlBucketError(errMsg: String) extends BucketErrorType("SQL_BUCKET_ERROR", errMsg)
  }
}