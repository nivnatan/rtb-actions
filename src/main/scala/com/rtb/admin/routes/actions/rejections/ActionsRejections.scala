package com.rtb.admin.routes.actions.rejections

import com.rtb.admin.routes.actions.constants.ActionRejectionTypes.ActionRejectionType
import com.rtb.admin.routes.actions.models.ActionRequest

/**
 * Created by Niv on 12/01/2024
 */
class ActionsRejections {

  trait AdminRejection {
    def rejectionType: ActionRejectionType
    def pass(actionRequest: ActionRequest): Boolean
  }
}
