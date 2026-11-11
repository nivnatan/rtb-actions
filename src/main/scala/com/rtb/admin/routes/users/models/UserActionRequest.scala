package com.rtb.admin.routes.users.models

import com.common.rtb.models.data.{RtbAdminUserData, RtbDspData, RtbSspData}
import com.common.utils.logging.DebugContext
import com.common.utils.time.MyTime
import com.rtb.admin.constants.Operations
import com.rtb.admin.routes.users.constants.UserActions.UserAction

/**
 * Created by Niv on 10/11/2026
 */
case class UserActionRequest(params: Map[String, String],
                             time: MyTime,
                             payload: String,
                             adminUserData: RtbAdminUserData,
                             action: UserAction,
                             operation: Option[Operations.Operation],
                             password: String,
                             dspData: Option[RtbDspData],
                             sspData: Option[RtbSspData],
                             debug: Option[DebugContext]
                            ) {
  def isDev: Boolean =
    params.get("isdev").contains("1")

  override def toString: String = {
    "\n\nUserActionRequest " + "{\n" +
      s"params=$params,\n" +
      s"adminUserData=${adminUserData.id},\n" +
      s"action=$action,\n" +
      s"operation=$operation,\n" +
      s"dspData=${dspData.map(_.id)},\n" +
      s"sspData=${sspData.map(_.id)},\n" +
      s"isDev=$isDev,\n" +
      s"time=$time\n}\n\n"
  }
}