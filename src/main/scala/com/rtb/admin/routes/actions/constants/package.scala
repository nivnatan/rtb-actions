package com.rtb.admin.routes.actions

import com.rtb.admin.routes.actions.models.ActionError

import scala.language.implicitConversions

/**
  * Created by Niv on 12/12/2021
  */
package object constants {

  object Actions {
    sealed abstract class Action(val name: String)
    case object BucketReplace extends Action("bucket-replace")
    case object BucketAdd extends Action("bucket-add")

    def getAction(name: String): Option[Action] = {
      name.toLowerCase match {
        case BucketReplace.name => Some(BucketReplace)
        case BucketAdd.name     => Some(BucketAdd)
        case _                  => None
      }
    }
  }

  object ActionRejectionTypes {
    sealed abstract class ActionRejectionType(val _id: String) {
      def id: String = s"ACTION_${_id}_REJECTION"
    }

    case object RateLimit extends ActionRejectionType("RATE_LIMIT")
  }

  object ActionErrors {

    sealed abstract class ActionErrorType(_id: String, val errorMsg: String) extends ActionError { override def id: String = s"ACTION_${_id}" }
    case class UnknownActionError(error: String) extends ActionErrorType("UNKNOWN_ERROR", error)
    case object UnknownActionType extends ActionErrorType("UNKNOWN_ACTION_TYPE", "unknown action type")

    object BucketsErrorTypes {
      sealed abstract class BucketErrorType(_id: String, errorMsg: String) extends ActionErrorType(s"BUCKET_${_id}", errorMsg)
      case object InvalidBucketParameters extends BucketErrorType("INVALID_BUCKET_PARAMETERS", "dsa")
      case class SqlBucketError(errMsg: String) extends BucketErrorType("SQL_BUCKET_ERROR", errMsg)
    }
  }
}
