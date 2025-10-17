package com.rtb.actions.routes.rejections

import com.rtb.actions.constants.ActionRejectionTypes.ActionRejectionType
import com.rtb.actions.routes.actions.models.ActionRequest

/**
 * Created by Niv on 12/01/2024
 */
class ActionsRejections {

  trait AdminRejection {
    def rejectionType: ActionRejectionType
    def pass(actionRequest: ActionRequest): Boolean
  }
}
