package com.rtb.admin

import akka.http.scaladsl.model.{HttpMethod, HttpMethods}

/**
 * Created by Niv on 11/11/2026
 */
package object constants {

  object Operations {
    sealed trait Operation
    case object Get     extends Operation
    case object Add     extends Operation
    case object Replace extends Operation
    case object Delete  extends Operation

    def from(m: HttpMethod): Option[Operation] = m match {
      case HttpMethods.GET    => Some(Get)
      case HttpMethods.POST   => Some(Add)
      case HttpMethods.PUT    => Some(Replace)
      case HttpMethods.DELETE => Some(Delete)
      case _                  => None
    }
  }
}
