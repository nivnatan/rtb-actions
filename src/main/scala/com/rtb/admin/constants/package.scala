package com.rtb.admin

import akka.http.scaladsl.server.Rejection
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
}
