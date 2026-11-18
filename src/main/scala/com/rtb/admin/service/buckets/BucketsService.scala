package com.rtb.admin.service.buckets

import com.common.clients.db.Database

/**
 * Created by Niv on 18/11/2026
 */
class BucketsService(rtbDb: Database, rtbDbDev: Database) {
  val bucketsAdd: BucketAdd = new BucketAdd(rtbDb, rtbDbDev)
  val bucketsReplace: BucketsReplace = new BucketsReplace(rtbDb, rtbDbDev)
  val bucketsDelete: BucketsDelete = new BucketsDelete(rtbDb, rtbDbDev)
  val bucketsGet: BucketsGet = new BucketsGet(rtbDb, rtbDbDev)
}
