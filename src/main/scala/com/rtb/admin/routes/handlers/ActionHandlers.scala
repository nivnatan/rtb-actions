package com.rtb.admin.routes.handlers

import com.rtb.admin.config.Config
import com.rtb.admin.constants.Actions.{Action, BucketAdd, BucketReplace}
import com.rtb.admin.routes.actions.models.{ActionRequest, ActionResult}
import com.rtb.admin.routes.handlers.buckets.BucketsReplaceHandler

/**
 * Created by Niv on 29/04/2022
 */
trait ActionHandler {
  def handle(actionRequest: ActionRequest): ActionResult
}

class ActionHandlers(config: Config) {

  private val bucketReplace = new BucketsReplaceHandler(config)

  def getHandler(action: Action): ActionHandler =
    action match {
      case BucketReplace => bucketReplace
      case BucketAdd     => bucketReplace
    }
}
