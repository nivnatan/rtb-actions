package com.rtb.actions.routes.handlers

import com.rtb.actions.config.Config
import com.rtb.actions.constants.Actions.{Action, BucketAdd, BucketReplace}
import com.rtb.actions.routes.actions.models.{ActionRequest, ActionResult}
import com.rtb.actions.routes.handlers.buckets.ReplaceHandler

/**
 * Created by Niv on 29/04/2022
 */
trait ActionHandler {
  def handle(actionRequest: ActionRequest): ActionResult
}

class ActionHandlers(config: Config) {

  private val bucketReplace = new ReplaceHandler(config)

  def getHandler(action: Action): ActionHandler =
    action match {
      case BucketReplace => bucketReplace
      case BucketAdd     => bucketReplace
    }
}
