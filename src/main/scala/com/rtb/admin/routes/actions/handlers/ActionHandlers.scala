package com.rtb.admin.routes.actions.handlers

import com.rtb.admin.config.Config
import com.rtb.admin.routes.actions.constants.Actions.{Action, BucketAdd, BucketDelete, BucketReplace}
import com.rtb.admin.routes.actions.handlers.buckets.{AddHandler, DeleteHandler, ReplaceHandler}
import com.rtb.admin.routes.actions.models.{ActionRequest, ActionResult}

/**
 * Created by Niv on 29/04/2022
 */
trait ActionHandler {
  def handle(actionRequest: ActionRequest): ActionResult
}

class ActionHandlers(config: Config) {

  private val bucketReplace    = new ReplaceHandler(config)
  private val bucketAdd        = new AddHandler(config)
  private val bucketDelete     = new DeleteHandler(config)

  def getHandler(action: Action): ActionHandler =
    action match {
      case BucketReplace => bucketReplace
      case BucketAdd     => bucketAdd
      case BucketDelete  => bucketDelete
    }
}
