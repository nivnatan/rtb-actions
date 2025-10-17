package com.rtb.tasks.routes.tasks.models

import com.common.utils.logging.DebugContext
import com.common.utils.time.MyTime

/**
  * Created by Niv on 11/12/2021
  */
case class TaskRequest(params: Map[String, String],
                       time: MyTime,
                       payload: String,
                       debug: Option[DebugContext]
                      ) {

  override def toString: String = {
    "\n\nTaskRequest " + "{\n" +
      s"params=$params,\n" +
      s"time=$time\n}\n\n"
  }
}