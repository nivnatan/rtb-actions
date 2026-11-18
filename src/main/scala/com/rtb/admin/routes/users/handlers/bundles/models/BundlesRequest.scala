package com.rtb.admin.routes.users.handlers.bundles.models

import com.rtb.admin.routes.users.models.UserActionRequest

/**
 * Created by Niv on 10/11/2026
 */
case class BundlesRequest(bucketId: Long, userActionRequest: UserActionRequest, bundles: Set[String])
