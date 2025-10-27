package com.rtb.admin.config

import akka.actor.{ActorContext, ActorSystem, Scheduler}
import com.common.clients.db.{Database, MySqlDatabase}
import com.common.clients.fluentd.FluentdLogger
import com.common.config.{CommonConfiguration, RtbConfigurations}
import com.common.rtb.constants.RtbConstants
import com.common.rtb.dao.{RtbDao, RtbDaoDatabase}
import com.common.utils.counters.{CountersBuilder, CountersHandler, CountersListenerFluentd}
import com.common.utils.http.models.HostData
import com.rtb.admin.AdminServer
import com.rtb.admin.utils.counters.Counters
import scala.concurrent.ExecutionContext

/**
  * Created by Niv on 11/12/2021
  */
trait ConfigSupport {
  def config: Config
  def fluentd: FluentdLogger = config.fluentd
  def countersHandler: CountersHandler = config.countersHandler
  def dao: RtbDao = config.dao
  def rtbDb: Database = config.rtbDb
  def rtbDbDev: Database = config.rtbDbDev
  implicit def system: ActorSystem = config.system
  implicit def ec: ExecutionContext = config.ec
  implicit def classLoader: ClassLoader = config.classLoader
  implicit def scheduler: Scheduler = system.scheduler
}

case class Config(context: ActorContext)(implicit val system: ActorSystem, val ec: ExecutionContext, val classLoader: ClassLoader) {
  val fluentd: FluentdLogger                    = new FluentdLogger("accesslog", Seq(HostData("localhost:24224")), bufferSize = 500000)
  val countersHandler: CountersHandler          = CountersBuilder(AdminServer.name).withServiceCounters(Counters.toSet).withListener(CountersListenerFluentd(fluentd, RtbConstants.RtbAdminMetricsLabel)).get
  val dao: RtbDao                               = new RtbDaoDatabase(CommonConfiguration.DaoRefreshIntervalSeconds, countersHandler)
  val rtbDb: Database                           = new MySqlDatabase(RtbConfigurations.DbUrl, RtbConfigurations.DbUserName, RtbConfigurations.DbPassword, countersHandler)
  val rtbDbDev: Database                        = new MySqlDatabase("jdbc:mysql://172.16.0.21:8672/rtb?useUnicode=true&characterEncoding=utf8", RtbConfigurations.DbUserName, RtbConfigurations.DbPassword, countersHandler)
}
