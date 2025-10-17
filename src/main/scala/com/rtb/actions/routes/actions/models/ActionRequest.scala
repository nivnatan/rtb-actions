package com.rtb.actions.routes.actions.models

import com.common.utils.logging.DebugContext
import com.common.utils.time.MyTime
import com.rtb.actions.constants.Actions.Action

/**
 * Created by Niv on 11/12/2021
 */
case class ActionRequest(params: Map[String, String],
                        time: MyTime,
                        payload: String,
                        action: Action,
                        debug: Option[DebugContext]
                      ) {

  override def toString: String = {
    "\n\nAdminRequest " + "{\n" +
      s"params=$params,\n" +
      s"action=$action,\n" +
      s"time=$time\n}\n\n"
  }
}